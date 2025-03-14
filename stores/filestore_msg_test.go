// Copyright 2016-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stores

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/stan.go/pb"
	"github.com/savelyev-an/nats-streaming-server/util"
)

const (
	testFSDefaultBackgroundTaskInterval = 15 * time.Millisecond
	testFSDefaultBufShrinkInterval      = 15 * time.Millisecond
	testFSDefaultCacheTTL               = int64(15 * time.Millisecond)
	testFSDefaultSliceCLoseInterval     = 15 * time.Millisecond
)

func init() {
	bufShrinkInterval = testFSDefaultBufShrinkInterval
	cacheTTL = testFSDefaultCacheTTL
	sliceCloseInterval = testFSDefaultSliceCLoseInterval
}

func TestFSBadMsgFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Create a valid store file first
	fs := createDefaultFileStore(t)

	cs := storeCreateChannel(t, fs, "foo")
	// Store a message
	storeMsg(t, cs, "foo", 1, []byte("msg"))

	msgStore := cs.Msgs.(*FileMsgStore)
	firstSliceFileName := msgStore.files[1].file.name
	firstIdxFileName := msgStore.files[1].idxFile.name

	// Close it
	fs.Close()

	//
	// INVALID INDEX FILE CONTENT
	//
	idxFile, err := openFileWithFlags(firstIdxFileName, os.O_RDWR)
	if err != nil {
		t.Fatalf("Error creating index file: %v", err)
	}
	if _, err := idxFile.Write([]byte("abcdefg")); err != nil {
		t.Fatalf("Error writing content: %v", err)
	}
	// Close the file
	if err := idxFile.Close(); err != nil {
		t.Fatalf("Unexpected error closing index file: %v", err)
	}
	// The index file will be deleted and recovery will be done
	// based on the data file, which should then work.
	fs, _ = openDefaultFileStore(t)
	fs.Close()

	// Corrupt data file. Index's last message will not match
	// data file, so idx file will be removed and recovery from
	// data file will be done, which should report failure.
	datContent, err := os.ReadFile(firstSliceFileName)
	if err != nil {
		t.Fatalf("Error reading %v: %v", firstSliceFileName, err)
	}
	if err := os.WriteFile(firstSliceFileName, datContent[:len(datContent)-5], 0666); err != nil {
		t.Fatalf("Error writing file %v: %v", firstSliceFileName, err)
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Index file should have been deleted from previous test

	// This will create the file without the file version
	if file, err := os.OpenFile(firstIdxFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating index file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	// Now for all other tests below, remove the index file so that
	// the server recovers the data file.
	if err := os.Remove(firstIdxFileName); err != nil {
		t.Fatalf("Unexpected error removing index file: %v", err)
	}

	// First delete the file...
	if err := os.Remove(firstSliceFileName); err != nil {
		t.Fatalf("Unable to delete the msg file %q: %v", firstSliceFileName, err)
	}
	// This will create the file without the file version
	if file, err := os.OpenFile(firstSliceFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		t.Fatalf("Error creating message data file file: %v", err)
	} else {
		file.Close()
	}
	// So we should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	resetToValidFile := func() *os.File {
		// First remove the file
		if err := os.Remove(firstSliceFileName); err != nil {
			stackFatalf(t, "Unexpected error removing file: %v", err)
		}
		// If present, remove the index file
		os.Remove(firstIdxFileName)
		// Create the file with proper file version
		file, err := openFile(firstSliceFileName)
		if err != nil {
			stackFatalf(t, "Error creating file: %v", err)
		}
		return file
	}

	//
	// INVALID CONTENT
	//
	file := resetToValidFile()
	if err := util.WriteInt(file, 5); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	if _, err := file.Write([]byte("hello")); err != nil {
		t.Fatalf("Error writing content: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	//
	// UNMARSHALL ERROR
	//
	file = resetToValidFile()
	msg := &pb.MsgProto{Sequence: 1, Data: []byte("this is a message")}
	b, _ := msg.Marshal()
	// overwrite with dummy content
	copy(b, []byte("hello"))
	// Write the header
	if err := util.WriteInt(file, len(b)); err != nil {
		t.Fatalf("Error writing header: %v", err)
	}
	// Write CRC
	if err := util.WriteInt(file, int(crc32.ChecksumIEEE(b))); err != nil {
		t.Fatalf("Unexpected error writing CRC: %v", err)
	}
	// Write content
	if _, err := file.Write(b); err != nil {
		t.Fatalf("Error writing info: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)

	//
	// ADD INVALID MESSAGE FILE NAME
	//
	os.Remove(firstSliceFileName)
	fileName := filepath.Join(testFSDefaultDatastore, "foo", msgFilesPrefix+"a"+datSuffix)
	file, err = openFile(fileName)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
	os.Remove(fileName)
	// Try with other malformed name
	fileName = filepath.Join(testFSDefaultDatastore, "foo", msgFilesPrefix+datSuffix[1:])
	file, err = openFile(fileName)
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	// Close the file
	if err := file.Close(); err != nil {
		t.Fatalf("Unexpected error closing file: %v", err)
	}
	// We should fail to create the filestore
	expectedErrorOpeningDefaultFileStore(t)
}

func TestFSStoreMsgCausesFlush(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t, BufferSize(50))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	m1 := storeMsg(t, cs, "foo", 1, []byte("hello"))
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	buffered := ms.bw.buf.Buffered()
	bufferedMsgs := len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != m1.Size()+recordHeaderSize {
		t.Fatalf("Expected buffered to be %v, got %v", m1.Size()+recordHeaderSize, buffered)
	}
	if bufferedMsgs != 1 {
		t.Fatalf("Expected 1 buffered message, got %v", bufferedMsgs)
	}

	m2 := storeMsg(t, cs, "foo", 2, []byte("hello again!"))
	ms.RLock()
	buffered = ms.bw.buf.Buffered()
	bufferedMsgs = len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != m2.Size()+recordHeaderSize {
		t.Fatalf("Expected buffered to be %v, got %v", m2.Size()+recordHeaderSize, buffered)
	}
	if bufferedMsgs != 1 {
		t.Fatalf("Expected 1 buffered message, got %v", bufferedMsgs)
	}

	// Now store a message that is bigger than the buffer, it should be
	// directly written to file
	payload := make([]byte, 200)
	storeMsg(t, cs, "foo", 3, payload)
	ms.RLock()
	buffered = ms.bw.buf.Buffered()
	bufferedMsgs = len(ms.bufferedMsgs)
	ms.RUnlock()
	if buffered != 0 {
		t.Fatalf("Expected buffered to be 0, got %v", buffered)
	}
	if bufferedMsgs != 0 {
		t.Fatalf("Expected 0 buffered message, got %v", bufferedMsgs)
	}
}

func TestFSRecoveryFileSlices(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", 1, []byte("msg1"))
	storeMsg(t, cs, "foo", 2, []byte("msg2"))

	// Close the store
	fs.Close()

	// Restart the store
	fs, state := openDefaultFileStore(t)
	defer fs.Close()

	cs = getRecoveredChannel(t, state, "foo")
	msgStore := cs.Msgs.(*FileMsgStore)

	// We should have moved to the second slice
	if msgStore.lastFSlSeq != 2 {
		t.Fatalf("Expected file slice to be the second one, got %v", msgStore.lastFSlSeq)
	}
}

func TestFSNoPanicAfterRestartWithSmallerLimits(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	fs.Close()

	limit := testDefaultStoreLimits
	limit.MaxMsgs = 100
	fs, err := NewFileStore(testLogger, testFSDefaultDatastore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	msg := []byte("hello")
	for i := 0; i < 50; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), msg)
	}

	fs.Close()

	limit.MaxMsgs = 10
	fs, err = NewFileStore(testLogger, testFSDefaultDatastore, &limit)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer fs.Close()
	state, err := fs.Recover()
	if err != nil {
		t.Fatalf("Unable to recover state: %v", err)
	}
	cs = getRecoveredChannel(t, state, "foo")
	for i := 0; i < 10; i++ {
		storeMsg(t, cs, "foo", uint64(i+51), msg)
	}

	first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
	expectedFirst := uint64(51)
	expectedLast := uint64(60)
	if first != expectedFirst || last != expectedLast {
		t.Fatalf("Expected first/last to be %v/%v, got %v/%v",
			expectedFirst, expectedLast, first, last)
	}
}

func TestFSFileSlicesClosed(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 50
	fs, err := NewFileStore(testLogger, testFSDefaultDatastore, &limits,
		SliceConfig(10, 0, 0, ""))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer fs.Close()
	payload := []byte("hello")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < limits.MaxMsgs; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), payload)
	}
	ms := cs.Msgs.(*FileMsgStore)
	ms.Flush()
	// Wait for cache to be empty
	timeout := time.Now().Add(time.Second)
	empty := false
	for time.Now().Before(timeout) {
		ms.RLock()
		empty = len(ms.cache.seqMaps) == 0
		ms.RUnlock()
		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !empty {
		t.Fatal("Cache should be empty")
	}
	for i := 0; i < limits.MaxMsgs; i++ {
		ms.Lookup(uint64(i + 1))
	}
	time.Sleep(450 * time.Millisecond)
	ms.RLock()
	for i, s := range ms.files {
		if s == ms.writeSlice {
			continue
		}
		if s.file.handle != nil {
			ms.RUnlock()
			t.Fatalf("File slice %v should be closed (data file)", i)
		}
		if s.idxFile.handle != nil {
			ms.RUnlock()
			t.Fatalf("File slice %v should be closed (index file)", i)
		}
	}
	ms.RUnlock()
}

func TestFSRecoverWithoutIndexFiles(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 8
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	total := limits.MaxMsgs + 1
	payload := []byte("hello")
	msgs := make([]*pb.MsgProto, 0, total)
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		msgs = append(msgs, storeMsg(t, cs, "foo", uint64(i+1), payload))
	}
	msgStore := cs.Msgs.(*FileMsgStore)
	// Get the index file names
	fs.RLock()
	idxFileNames := make([]string, 0, len(msgStore.files))
	for _, sl := range msgStore.files {
		idxFileNames = append(idxFileNames, sl.idxFile.name)
	}
	fs.RUnlock()
	// Close store
	fs.Close()

	// Remove the index files
	for _, fn := range idxFileNames {
		if err := os.Remove(fn); err != nil {
			t.Fatalf("Error removing file %q: %v", fn, err)
		}
	}
	// Restart store
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	cs = getRecoveredChannel(t, state, "foo")
	for i := 0; i < total; i++ {
		m := msgStoreLookup(t, cs.Msgs, uint64(i+1))
		if !reflect.DeepEqual(m, msgs[i]) {
			t.Fatalf("Expected to get message %v, got %v", msgs[i], m)
		}
	}
}

func TestFSEmptySlice(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	m := storeMsg(t, cs, "foo", 1, []byte("hello"))

	fs.Close()

	// Add an empty slice
	file, err := openFile(filepath.Join(testFSDefaultDatastore, "foo", msgFilesPrefix+"2"+datSuffix))
	if err != nil {
		t.Fatalf("Error creating file: %v", err)
	}
	file.Close()

	// Recover
	fs, state := openDefaultFileStore(t)
	defer fs.Close()

	cs = getRecoveredChannel(t, state, "foo")
	lm := msgStoreLookup(t, cs.Msgs, 1)
	if err != nil {
		t.Fatalf("Error getting message 1: %v", err)
	}
	if !reflect.DeepEqual(m, lm) {
		t.Fatalf("Expected recovered message to be %v, got %v", m, lm)
	}
}

func TestFSRemoveFileSlices(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Set config such that each slice store only 1 message
	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	limits := DefaultStoreLimits
	// Ensure that slices will be removed.
	limits.MaxMsgs = 3
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	expectedFirst := uint64(5)
	total := 7
	payload := []byte("hello")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), payload)
	}
	// Check first and last indexes
	ms := cs.Msgs.(*FileMsgStore)
	if m := msgStoreFirstMsg(t, ms); m.Sequence != expectedFirst {
		t.Fatalf("Expected message sequence to be %v, got %v", expectedFirst, m.Sequence)
	}
	if m := msgStoreLastMsg(t, ms); m.Sequence != uint64(total) {
		t.Fatalf("Expected message sequence to be %v, got %v", total, m.Sequence)
	}
	// Close store
	fs.Close()

	// Reopen
	fs, state := openDefaultFileStore(t)
	defer fs.Close()
	cs = getRecoveredChannel(t, state, "foo")
	ms = cs.Msgs.(*FileMsgStore)
	if m := msgStoreFirstMsg(t, ms); m.Sequence != expectedFirst {
		t.Fatalf("Expected message sequence to be %v, got %v", expectedFirst, m.Sequence)
	}
	if m := msgStoreLastMsg(t, ms); m.Sequence != uint64(total) {
		t.Fatalf("Expected message sequence to be %v, got %v", total, m.Sequence)
	}
}

func TestFSFirstEmptySliceRemovedOnCreateNewSlice(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, time.Second, ""))
	defer fs.Close()

	limits := DefaultStoreLimits
	limits.MaxAge = time.Second
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Unexpected error setting limits: %v", err)
	}

	cs := storeCreateChannel(t, fs, "foo")
	// Store a message
	storeMsg(t, cs, "foo", 1, []byte("test"))

	// Wait for message to expire
	timeout := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		if n, _ := msgStoreState(t, cs.Msgs); n == 0 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Message should have expired")
	}

	// First slice should still exist although empty
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	numFiles := len(ms.files)
	firstFileSeq := ms.firstFSlSeq
	empty := false
	if ms.writeSlice != nil && ms.writeSlice.msgsCount == ms.writeSlice.rmCount {
		empty = true
	}
	firstWrite := ms.writeSlice.firstWrite
	ms.RUnlock()
	if !empty || numFiles != 1 || firstFileSeq != 1 {
		t.Fatalf("Expected slice to be empty, numFiles and firstFileSeq to be 1, got %v, %v and %v",
			empty, numFiles, firstFileSeq)
	}

	// Since slice time check uses ms.timeTick, ensure that we wait long enough.
	timeout = time.Now().Add(5 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		timeTick := atomic.LoadInt64(&ms.timeTick)

		if timeTick-firstWrite > int64(time.Second) {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Waited too long for timeTick to update")
	}

	// Send another message...
	storeMsg(t, cs, "foo", 2, []byte("test"))

	timeout = time.Now().Add(5 * time.Second)
	ok = false
	for time.Now().Before(timeout) {
		if n, _ := msgStoreState(t, cs.Msgs); n == 1 {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Should have gotten a message")
	}

	// A new slice should have been created and the first one deleted.
	ms.RLock()
	numFiles = len(ms.files)
	firstFileSeq = ms.firstFSlSeq
	updatedwriteSlice := ms.writeSlice == ms.files[2]
	ms.RUnlock()
	if !updatedwriteSlice || numFiles != 1 || firstFileSeq != 2 {
		t.Fatalf("Expected current slice to be updated to second slice, numFiles to be 1, firstFileSeq to be 2, got %v, %v and %v",
			updatedwriteSlice, numFiles, firstFileSeq)
	}
}

func TestFSMsgStoreVariousBufferSizes(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	sizes := []int{0, msgBufMinShrinkSize - msgBufMinShrinkSize/10, msgBufMinShrinkSize, 3*msgBufMinShrinkSize + msgBufMinShrinkSize/2}
	for _, size := range sizes {

		// Create a store with buffer writer of the given size
		fs := createDefaultFileStore(t, BufferSize(size))
		defer fs.Close()

		seq := uint64(1)
		cs := storeCreateChannel(t, fs, "foo")
		storeMsg(t, cs, "foo", seq, []byte("hello"))
		seq++

		// Get FileMsgStore
		ms := cs.Msgs.(*FileMsgStore)

		// Cause a flush to empty the buffer
		ms.Flush()

		// Check that bw is not nil and writer points to the buffer writer
		ms.RLock()
		bw := ms.bw
		writer := ms.writer
		file := ms.writeSlice.file.handle
		bufSize := 0
		if ms.bw != nil {
			bufSize = ms.bw.buf.Available()
		}
		ms.RUnlock()
		if size == 0 {
			if bw != nil {
				t.Fatal("FileMsgStore's buffer writer should be nil")
			}
		} else if bw == nil {
			t.Fatal("FileMsgStore's buffer writer should not be nil")
		}
		if size == 0 {
			if writer != file {
				t.Fatal("FileMsgStore's writer should be set to file")
			}
		} else if writer != bw.buf {
			t.Fatal("FileMsgStore's writer should be set to the buffer writer")
		}
		initialSize := size
		if size > msgBufMinShrinkSize {
			initialSize = msgBufMinShrinkSize
		}
		if bufSize != initialSize {
			t.Fatalf("Incorrect initial size, should be %v, got %v", initialSize, bufSize)
		}

		// Fill up the buffer (meaningfull only when buffer is used)
		fillBuffer := func() {
			total := 0
			for i := 0; i < 1000; i++ {
				ms.RLock()
				before := ms.bw.buf.Buffered()
				ms.RUnlock()
				storeMsg(t, cs, "foo", seq, []byte("hello"))
				ms.RLock()
				seq++
				if ms.bw.buf.Buffered() > before {
					total += ms.bw.buf.Buffered() - before
				} else {
					total += ms.bw.buf.Buffered()
				}
				ms.RUnlock()
				// Stop when we have persisted at least 2 times the max buffer size
				if total >= 2*size {
					// We should have caused buffer to be flushed by now
					break
				}
			}
			if total < 2*size {
				t.Fatalf("Did not reach target total (%v, got %v) after limit iterations", 2*size, total)
			}
		}
		if size > 0 {
			fillBuffer()
		} else {
			// Just write a bunch of stuff
			for i := 0; i < 50; i++ {
				storeMsg(t, cs, "foo", seq, []byte("hello"))
				seq++
			}
		}

		ms.RLock()
		bufSize = 0
		if size > 0 {
			bufSize = ms.bw.bufSize
		}
		ms.RUnlock()
		if size == 0 {
			if bufSize != 0 {
				t.Fatalf("BufferSize is 0, so ss.bufSize should be 0, got %v", bufSize)
			}
		} else if size < msgBufMinShrinkSize {
			// If size is smaller than min shrink size, the buffer should not have
			// increased in size
			if bufSize > msgBufMinShrinkSize {
				t.Fatalf("BufferSize=%v - ss.bw size should at or below %v, got %v", size, msgBufMinShrinkSize, bufSize)
			}
		} else {
			// We should have started at min size, and now size should have been increased.
			if bufSize < msgBufMinShrinkSize || bufSize > size {
				t.Fatalf("BufferSize=%v - ss.bw size should have increased but no more than %v, got %v", size, size, bufSize)
			}
		}

		// When buffer size is greater than min size, see if it shrinks
		if size > msgBufMinShrinkSize {
			// Invoke the timer callback manually (so we don't have to wait)
			// Call many times and make sure size never goes down too low.
			for i := 0; i < 14; i++ {
				ms.Lock()
				ms.bw.tryShrinkBuffer(ms.writeSlice.file.handle)
				ms.writer = ms.bw.buf
				ms.Unlock()
			}
			// Now check
			ms.RLock()
			bufSizeNow := ms.bw.bufSize
			ms.RUnlock()
			if bufSizeNow >= bufSize {
				t.Fatalf("Buffer size expected to decrease, got: %v", bufSizeNow)
			}
			if bufSizeNow < msgBufMinShrinkSize {
				t.Fatalf("Buffer should not go below %v, got %v", msgBufMinShrinkSize, bufSizeNow)
			}

			// Check that the request to shrink is canceled if more data arrive
			// First make buffer expand.
			fillBuffer()
			// Flush to empty it
			ms.Flush()
			// Invoke shrink
			ms.Lock()
			ms.bw.tryShrinkBuffer(ms.writeSlice.file.handle)
			ms.Unlock()
			// Check that request is set
			ms.RLock()
			shrinkReq := ms.bw.shrinkReq
			ms.RUnlock()
			if !shrinkReq {
				t.Fatal("Shrink request should be true")
			}
			// Cause buffer to expand again
			fillBuffer()
			// Check that request should have been canceled.
			ms.RLock()
			shrinkReq = ms.bw.shrinkReq
			ms.RUnlock()
			if shrinkReq {
				t.Fatal("Shrink request should be false")
			}
		}
		fs.Close()
		cleanupFSDatastore(t)
	}
}

func TestFSArchiveScript(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	tmpDir, err := os.MkdirTemp(".", "")
	if err != nil {
		t.Fatalf("Unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Unable to get current directory: %v", err)
	}
	scriptFile := ""
	content := ""
	if runtime.GOOS == "windows" {
		scriptFile = fmt.Sprintf("%s\\script_%v.bat", pwd, time.Now().UnixNano())
		content = fmt.Sprintf("mkdir %s\\%s\\%%1\nmove %%2 %s\\%s\\%%1\nmove %%3 %s\\%s\\%%1", pwd, tmpDir, pwd, tmpDir, pwd, tmpDir)
	} else {
		scriptFile = fmt.Sprintf("%s/script_%v.sh", pwd, time.Now().UnixNano())
		content = fmt.Sprintf("#!/bin/bash\nmkdir -p %s/%s/$1\nmv $2 $3 %s/%s/$1\n", pwd, tmpDir, pwd, tmpDir)
	}
	if err := os.WriteFile(scriptFile, []byte(content), 0777); err != nil {
		t.Fatalf("Error creating script: %v", err)
	}
	defer os.Remove(scriptFile)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, scriptFile))
	defer fs.Close()
	limits := DefaultStoreLimits
	limits.MaxMsgs = 1
	fs.SetLimits(&limits)

	cs := storeCreateChannel(t, fs, "foo")
	// Store one message
	storeMsg(t, cs, "foo", 1, []byte("msg1"))

	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	fileName := ms.files[1].file.name
	ms.RUnlock()

	// Store one more message. Should move to next slice and invoke script
	// for first slice.
	storeMsg(t, cs, "foo", 2, []byte("msg2"))

	// Original file should not be present
	ok := false
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(fileName); s == nil || serr != nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}

	// File should have been moved to tmpDir by script
	ok = false
	bakFile := fmt.Sprintf("%s/foo/%s1%s%s", tmpDir, msgFilesPrefix, datSuffix, bakSuffix)
	timeout = time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(bakFile); s != nil && serr == nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File should have been moved to tmp dir")
	}
	// Close store
	fs.Close()
	// Cleanup datastore
	cleanupFSDatastore(t)

	// Create a script that will error out
	os.Remove(scriptFile)
	content = "xxx"
	if err := os.WriteFile(scriptFile, []byte(content), 0777); err != nil {
		t.Fatalf("Error creating script: %v", err)
	}
	defer os.Remove(scriptFile)

	fs = createDefaultFileStore(t, SliceConfig(0, 0, 0, scriptFile))
	defer fs.Close()
	fs.SetLimits(&limits)

	cs = storeCreateChannel(t, fs, "foo")
	// Store one message
	storeMsg(t, cs, "foo", 1, []byte("msg1"))

	// Store one more message. Should move to next slice and invoke script
	// for first slice.
	storeMsg(t, cs, "foo", 2, []byte("msg2"))

	// Original file should not be present
	ok = false
	timeout = time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(fileName); s == nil || serr != nil {
			ok = true
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}

	// Since script should fail, .bak file should still be in channel's directory
	ok = true
	timeout = time.Now().Add(time.Second)
	for time.Now().Before(timeout) {
		if s, serr := os.Stat(bakFile); s == nil || serr != nil {
			ok = false
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !ok {
		t.Fatal("File still present in channel directory")
	}
}

func TestFSNoSliceLimitAndNoChannelLimits(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// No slice limit
	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	// And no channel limit
	limits := StoreLimits{}
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	total := 1000
	msg := []byte("msg")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), msg)
	}

	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	numFiles := len(ms.files)
	firstFileSeq := ms.firstFSlSeq
	lastFileSeq := ms.lastFSlSeq
	ms.RUnlock()

	if numFiles != 1 || firstFileSeq != 1 || lastFileSeq != 1 {
		t.Fatalf("Expected numFiles, firstFileSeq and lastFileSeq to be all 1, got %v, %v and %v",
			numFiles, firstFileSeq, lastFileSeq)
	}
}

func TestFSMsgRemovedWhileBuffered(t *testing.T) {
	// Test is irrelevant if no buffering used
	if testFSDisableBufferWriters {
		t.SkipNow()
	}
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	limits := DefaultStoreLimits
	limits.MaxMsgs = 10
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting limits: %v", &limits)
	}

	total := 1000
	msg := []byte("msg")
	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), msg)
	}

	fs.Close()

	fs, state := newFileStore(t, testFSDefaultDatastore, &limits)
	defer fs.Close()
	if state == nil {
		t.Fatal("Expected to recover a state")
	}
}

func TestFSSliceLimitsBasedOnChannelLimits(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	// First check that with low channel limits, we have at least
	// a slice of 1.
	limits := DefaultStoreLimits
	limits.MaxMsgs = 3
	limits.MaxBytes = 3
	limits.MaxAge = 3 * time.Second
	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", 1, []byte("msg"))

	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	slCount := ms.slCountLim
	slSize := ms.slSizeLim
	slAge := ms.slAgeLim
	ms.RUnlock()

	if slCount != 1 {
		t.Fatalf("Expected slice limit count to be 1, got %v", slCount)
	}
	if slSize != 1 {
		t.Fatalf("Expected slice limit size to be 1, got %v", slSize)
	}
	if slAge != int64(time.Second) {
		t.Fatalf("Expected slice limit age to be 1sec, got %v", time.Duration(slAge))
	}
	fs.Close()
	cleanupFSDatastore(t)

	// Open with different limits
	limits.MaxMsgs = 100
	limits.MaxBytes = 100
	limits.MaxAge = 20 * time.Second
	fs = createDefaultFileStore(t, SliceConfig(0, 0, 0, ""))
	defer fs.Close()

	if err := fs.SetLimits(&limits); err != nil {
		t.Fatalf("Error setting file limits: %v", err)
	}

	cs = storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", 2, []byte("msg"))

	ms = cs.Msgs.(*FileMsgStore)
	ms.RLock()
	slCount = ms.slCountLim
	slSize = ms.slSizeLim
	slAge = ms.slAgeLim
	ms.RUnlock()

	if slCount != 25 {
		t.Fatalf("Expected slice limit count to be 25, got %v", slCount)
	}
	if slSize != 25 {
		t.Fatalf("Expected slice limit size to be 25, got %v", slSize)
	}
	if slAge != int64(5*time.Second) {
		t.Fatalf("Expected slice limit age to be 5sec, got %v", time.Duration(slAge))
	}
}

func TestFSRecoverSlicesOutOfOrder(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Make a slice hold only 1 message
	fs := createDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	msg := []byte("msg")
	total := 200
	cs := storeCreateChannel(t, fs, "foo")
	// Create slices
	for i := 0; i < total; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), msg)
	}

	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	firstFileSeq, lastFileSeq := ms.firstFSlSeq, ms.lastFSlSeq
	first, last := ms.first, ms.last
	wOffset := ms.wOffset
	ms.RUnlock()

	if first != 1 || last != uint64(total) {
		t.Fatalf("Expected first and last to be (1,%v), got (%v,%v)", total, first, last)
	}
	if firstFileSeq != 1 || lastFileSeq != total {
		t.Fatalf("Expected first and last file sequence to be (1,%v), got (%v,%v)", total, firstFileSeq, lastFileSeq)
	}

	fs.Close()

	fs, state := openDefaultFileStore(t, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	cs = getRecoveredChannel(t, state, "foo")
	ms = cs.Msgs.(*FileMsgStore)
	ms.RLock()
	firstFileSeq, lastFileSeq = ms.firstFSlSeq, ms.lastFSlSeq
	first, last = ms.first, ms.last
	writeSlice := ms.writeSlice
	recoveredWOffset := ms.wOffset
	ms.RUnlock()

	if first != 1 || last != uint64(total) {
		t.Fatalf("Expected first and last to be (1,%v), got (%v,%v)", total, first, last)
	}
	if firstFileSeq != 1 || lastFileSeq != total {
		t.Fatalf("Expected first and last file sequence to be (1,%v), got (%v,%v)", total, firstFileSeq, lastFileSeq)
	}
	if recoveredWOffset != wOffset {
		t.Fatalf("Write offset should be %v, got %v", wOffset, recoveredWOffset)
	}
	if writeSlice == nil || writeSlice.firstSeq != uint64(total) {
		t.Fatalf("Unexpected current slice: %v", writeSlice)
	}
}

func TestFSBufShrink(t *testing.T) {
	if testFSDisableBufferWriters {
		t.SkipNow()
	}
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t, BufferSize(5*1024*1024))
	defer fs.Close()

	msg := make([]byte, 1024*1024)
	cs := storeCreateChannel(t, fs, "foo")
	storeMsg(t, cs, "foo", 1, msg)

	ms := cs.Msgs.(*FileMsgStore)
	// Check that buffer size is at least 1MB
	ms.RLock()
	bufSize := ms.bw.bufSize
	ms.RUnlock()
	if bufSize < 1024*1024 {
		t.Fatalf("Expected buffer to be at least 1MB, got %v", bufSize)
	}
	// Flush the store to empty the buffer
	if err := cs.Msgs.Flush(); err != nil {
		t.Fatalf("Error flushing store: %v", err)
	}
	// Ensure that buffer shrinks
	timeout := time.Now().Add(5 * time.Second)
	ok := false
	for time.Now().Before(timeout) {
		ms.RLock()
		newBufSize := ms.bw.bufSize
		ms.RUnlock()
		if newBufSize < bufSize {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("Buffer did not shrink")
	}
}

func TestFSCacheList(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// Increase cacheTTL so eviction does not happen while we test content of list
	cacheTTL = int64(10 * time.Second)
	defer func() {
		cacheTTL = int64(testFSDefaultCacheTTL)
	}()

	fs := createDefaultFileStore(t)
	defer fs.Close()

	msg := []byte("hello")
	cs := storeCreateChannel(t, fs, "foo")
	// Store messages 1, 2, 3
	for i := 0; i < 3; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), msg)
	}

	ms := cs.Msgs.(*FileMsgStore)

	// Check list content
	checkList := func(expectedSeqs ...uint64) {
		ms.RLock()
		c := ms.cache
		cMsg := c.head
		i := 0
		good := 0
		gotStr := ""
		for cMsg != nil {
			gotStr = fmt.Sprintf("%v%v ", gotStr, cMsg.msg.Sequence)
			if cMsg.msg.Sequence == expectedSeqs[i] {
				good++
			}
			i++
			cMsg = cMsg.next
		}
		ms.RUnlock()
		if i != len(expectedSeqs) || good != len(expectedSeqs) {
			expectedStr := ""
			for i := 0; i < len(expectedSeqs); i++ {
				expectedStr = fmt.Sprintf("%v%v ", expectedStr, expectedSeqs[i])
			}
			stackFatalf(t, "Expected sequences: %q, got %q", expectedStr, gotStr)
		}
	}
	// Check that we should have 1, 2, 3
	checkList(1, 2, 3)
	// Lookup first, should be moved to end of list
	ms.Lookup(1)
	checkList(2, 3, 1)
	// Repeat...
	ms.lookup(2)
	checkList(3, 1, 2)
	ms.Lookup(3)
	checkList(1, 2, 3)
	// Lookup last should leave it there
	ms.Lookup(3)
	checkList(1, 2, 3)
}

func TestFSMsgCache(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	// For this test, increase a bit the test values
	cacheTTL = int64(250 * time.Millisecond)
	defer func() {
		cacheTTL = testFSDefaultCacheTTL
	}()

	fs := createDefaultFileStore(t)
	defer fs.Close()

	payload := []byte("data")
	cs := storeCreateChannel(t, fs, "foo")
	seq := uint64(1)
	msg := storeMsg(t, cs, "foo", seq, payload)
	seq++

	ms := cs.Msgs.(*FileMsgStore)
	// Wait for stored message to be removed from cache
	timeout := time.Now().Add(time.Second)
	empty := false
	for time.Now().Before(timeout) {
		ms.RLock()
		empty = len(ms.cache.seqMaps) == 0
		ms.RUnlock()
		if empty {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !empty {
		t.Fatal("Message not removed from cache")
	}
	// First lookup
	lm := msgStoreLookup(t, ms, msg.Sequence)
	if !reflect.DeepEqual(msg, lm) {
		t.Fatalf("Expected lookup message to be %v, got %v", msg, lm)
	}
	// Flush store so we removed from buffered messages
	ms.Flush()
	// As long as we call lookup, message should stay in cache
	closeFile := true
	end := time.Now().Add(2 * time.Duration(cacheTTL))
	for time.Now().Before(end) {
		lm := msgStoreLookup(t, ms, msg.Sequence)
		if !reflect.DeepEqual(msg, lm) {
			t.Fatalf("Expected lookup message to be %v, got %v", msg, lm)
		}
		if closeFile {
			ms.Lock()
			ms.writeSlice.file.handle.Close()
			ms.Unlock()
			closeFile = false
		} else {
			time.Sleep(15 * time.Millisecond)
		}
	}
	// Wait for a bit.
	time.Sleep(bkgTasksSleepDuration + time.Duration(cacheTTL) + 500*time.Millisecond)
	// Now a lookup should return nil because message
	// should have been evicted and file is closed
	lm, err := ms.Lookup(msg.Sequence)
	if lm != nil || err == nil {
		t.Fatalf("Unexpected message: %v", lm)
	}

	// Use another channel
	end = time.Now().Add(2 * bkgTasksSleepDuration)
	i := 0
	cs = storeCreateChannel(t, fs, "bar")
	for time.Now().Before(end) {
		storeMsg(t, cs, "bar", seq, payload)
		seq++
		i++
		if i == 100 {
			time.Sleep(15 * time.Millisecond)
		}
	}
	time.Sleep(bkgTasksSleepDuration)
	// Cache should be empty now
	ms.RLock()
	empty = len(ms.cache.seqMaps) == 0
	ms.RUnlock()
	if !empty {
		t.Fatal("Cache should be empty")
	}
}

func TestFSMsgStoreBackgroundTaskCrash(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	fs := createDefaultFileStore(t)
	defer fs.Close()

	storeCreateChannel(t, fs, "foo")
	// Wait for background task to execute
	time.Sleep(50 * time.Millisecond)
	// It should not have crashed.
}

func TestFSPanicOnStoreCloseWhileMsgsExpire(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxAge = 30 * time.Millisecond

	fs, _ := newFileStore(t, testFSDefaultDatastore, &limits)
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")

	for i := 0; i < 100; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), []byte("msg"))
	}

	time.Sleep(30 * time.Millisecond)
	fs.Close()
}

func TestFSPanicOnMsgExpireWithClosedDatFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	limits := testDefaultStoreLimits
	limits.MaxAge = 500 * time.Millisecond

	fs, _ := newFileStore(t, testFSDefaultDatastore, &limits, SliceConfig(1, 0, 0, ""))
	defer fs.Close()

	cs := storeCreateChannel(t, fs, "foo")
	for i := 0; i < 3; i++ {
		storeMsg(t, cs, "foo", uint64(i+1), []byte("msg"))
	}

	ms := cs.Msgs.(*FileMsgStore)
	ms.Lock()
	idxFileName := ms.files[1].idxFile.name
	os.Remove(idxFileName)
	err := os.WriteFile(idxFileName, []byte("xxxxxxx"), 0666)
	ms.Unlock()
	if err != nil {
		t.Fatalf("Error rewriting index file: %v", err)
	}

	time.Sleep(750 * time.Millisecond)
}

func TestFSMsgIndexFileWithExtraZeros(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ms := c.Msgs
	msg1 := storeMsg(t, c, "foo", 1, []byte("msg1"))
	ms.(*FileMsgStore).RLock()
	fname := ms.(*FileMsgStore).writeSlice.idxFile.name
	ms.(*FileMsgStore).RUnlock()
	s.Close()

	f, err := openFileWithFlags(fname, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	b := make([]byte, msgIndexRecSize)
	if _, err := f.Write(b); err != nil {
		t.Fatalf("Error adding zeros: %v", err)
	}
	f.Close()

	// Reopen file store
	s, rs := openDefaultFileStore(t)
	defer s.Close()
	rc := getRecoveredChannel(t, rs, "foo")
	msg := msgStoreLookup(t, rc.Msgs, msg1.Sequence)
	if !reflect.DeepEqual(msg, msg1) {
		t.Fatalf("Expected message %v, got %v", msg1, msg)
	}
	// Add one more message
	msg2 := storeMsg(t, rc, "foo", 2, []byte("msg2"))
	s.Close()

	// Reopen file store
	s, rs = openDefaultFileStore(t)
	defer s.Close()
	rc = getRecoveredChannel(t, rs, "foo")
	msgs := []*pb.MsgProto{msg1, msg2}
	for _, omsg := range msgs {
		msg := msgStoreLookup(t, rc.Msgs, omsg.Sequence)
		if !reflect.DeepEqual(msg, omsg) {
			t.Fatalf("Expected message %v, got %v", omsg, msg)
		}
	}
}

func TestFSMsgFileWithExtraZeros(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ms := c.Msgs
	msg1 := storeMsg(t, c, "foo", 1, []byte("msg1"))
	ms.(*FileMsgStore).RLock()
	datname := ms.(*FileMsgStore).writeSlice.file.name
	idxname := ms.(*FileMsgStore).writeSlice.idxFile.name
	ms.(*FileMsgStore).RUnlock()
	s.Close()

	// Remove index file to make store use dat file on recovery
	os.Remove(idxname)
	// Add zeros at end of datafile
	f, err := openFileWithFlags(datname, os.O_CREATE|os.O_RDWR|os.O_APPEND)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	b := make([]byte, recordHeaderSize)
	if _, err := f.Write(b); err != nil {
		t.Fatalf("Error adding zeros: %v", err)
	}
	f.Close()

	// Reopen file store
	s, rs := openDefaultFileStore(t)
	defer s.Close()
	rc := getRecoveredChannel(t, rs, "foo")
	msg := msgStoreLookup(t, rc.Msgs, msg1.Sequence)
	if !reflect.DeepEqual(msg, msg1) {
		t.Fatalf("Expected message %v, got %v", msg1, msg)
	}
	// Add one more message
	msg2 := storeMsg(t, rc, "foo", 2, []byte("msg2"))
	s.Close()

	// Reopen file store
	s, rs = openDefaultFileStore(t)
	defer s.Close()
	rc = getRecoveredChannel(t, rs, "foo")
	msgs := []*pb.MsgProto{msg1, msg2}
	for _, omsg := range msgs {
		msg := msgStoreLookup(t, rc.Msgs, omsg.Sequence)
		if !reflect.DeepEqual(msg, omsg) {
			t.Fatalf("Expected message %v, got %v", omsg, msg)
		}
	}
}

type testFSGapsOption struct {
	name string
	opt  FileStoreOption
}

func testFSGetOptionsForGapsTests() []testFSGapsOption {
	defaultOptions := DefaultFileStoreOptions
	opts := []testFSGapsOption{
		{"Default", AllOptions(&defaultOptions)},
		{"NoBuffer", BufferSize(0)},
	}
	return opts
}

func TestFSGapsInSequence(t *testing.T) {
	opts := testFSGetOptionsForGapsTests()
	for _, o := range opts {
		t.Run(o.name, func(t *testing.T) {

			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			s := createDefaultFileStore(t, o.opt)
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")

			payload := []byte("msg")

			// storeMsg calls Store and then Lookup
			storeMsg(t, cs, "foo", 1, payload)
			storeMsg(t, cs, "foo", 2, payload)
			storeMsg(t, cs, "foo", 5, payload)

			s.Close()
			s, state := openDefaultFileStore(t, o.opt)
			defer s.Close()
			cs = getRecoveredChannel(t, state, "foo")

			for i := 1; i <= 5; i++ {
				m := msgStoreLookup(t, cs.Msgs, uint64(i))
				if i >= 3 && i <= 4 {
					if len(m.Data) != 0 {
						stackFatalf(t, "For seq %v, expected empty message, got %v", i, m)
					}
				} else {
					if m == nil || len(m.Data) == 0 {
						stackFatalf(t, "For seq %v expected message, got nil", i)
					}
				}
			}
		})
	}
}

func TestFSGapsInSequenceWithMaxMsgsLimits(t *testing.T) {
	opts := testFSGetOptionsForGapsTests()
	for _, o := range opts {
		t.Run(o.name, func(t *testing.T) {

			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			s := createDefaultFileStore(t, o.opt)
			defer s.Close()

			limits := testDefaultStoreLimits
			limits.MaxMsgs = 3
			if err := s.SetLimits(&limits); err != nil {
				t.Fatalf("Error setting limits: %v", err)
			}

			cs := storeCreateChannel(t, s, "foo")

			payload := []byte("msg")
			storeMsg(t, cs, "foo", 1, payload)
			storeMsg(t, cs, "foo", 2, payload)
			storeMsg(t, cs, "foo", 5, payload)

			s.Close()
			s, state := openDefaultFileStoreWithLimits(t, &limits, o.opt)
			defer s.Close()

			cs = getRecoveredChannel(t, state, "foo")
			n, _ := msgStoreState(t, cs.Msgs)
			if n != 3 {
				t.Fatalf("Expected 3 messages, got %v", n)
			}

			storeMsg(t, cs, "foo", 6, payload)
			storeMsg(t, cs, "foo", 7, payload)
			storeMsg(t, cs, "foo", 8, payload)

			n, _ = msgStoreState(t, cs.Msgs)
			if n != 3 {
				t.Fatalf("Expected 3 messages, got %v", n)
			}
			first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
			if first != 6 || last != 8 {
				t.Fatalf("Unexpected first/last: %v/%v", first, last)
			}
		})
	}
}

func TestFSGapsInSequenceWithExpirationLimits(t *testing.T) {
	opts := testFSGetOptionsForGapsTests()
	for _, o := range opts {
		t.Run(o.name, func(t *testing.T) {

			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			s := createDefaultFileStore(t, o.opt)
			defer s.Close()

			limits := testDefaultStoreLimits
			limits.MaxAge = 100 * time.Millisecond
			if err := s.SetLimits(&limits); err != nil {
				t.Fatalf("Error setting limits: %v", err)
			}

			cs := storeCreateChannel(t, s, "foo")

			payload := []byte("msg")
			storeMsg(t, cs, "foo", 1, payload)
			storeMsg(t, cs, "foo", 2, payload)
			storeMsg(t, cs, "foo", 5, payload)

			time.Sleep(200 * time.Millisecond)

			n, b := msgStoreState(t, cs.Msgs)
			if n != 0 || b != 0 {
				t.Fatalf("Expected no message, got %v/%v", n, b)
			}

			storeMsg(t, cs, "foo", 6, payload)
			n, b = msgStoreState(t, cs.Msgs)
			if n != 1 || b == 0 {
				t.Fatalf("Expected 1 message, got %v/%v", n, b)
			}
		})
	}
}

func TestFSGapsInSequenceWithSliceMaxMsgsLimits(t *testing.T) {
	opts := testFSGetOptionsForGapsTests()
	for _, o := range opts {
		t.Run(o.name, func(t *testing.T) {

			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			s := createDefaultFileStore(t, o.opt, SliceConfig(3, 0, 0, ""))
			defer s.Close()

			cs := storeCreateChannel(t, s, "foo")

			payload := []byte("msg")
			storeMsg(t, cs, "foo", 1, payload)
			storeMsg(t, cs, "foo", 2, payload)
			storeMsg(t, cs, "foo", 5, payload)

			n, _ := msgStoreState(t, cs.Msgs)
			// Gaps are still counted as messages
			if n != 5 {
				t.Fatalf("Expected 5 messages, got %v", n)
			}

			storeMsg(t, cs, "foo", 6, payload)
			storeMsg(t, cs, "foo", 7, payload)
			storeMsg(t, cs, "foo", 8, payload)

			n, _ = msgStoreState(t, cs.Msgs)
			if n != 8 {
				t.Fatalf("Expected 8 messages, got %v", n)
			}
			first, last := msgStoreFirstAndLastSequence(t, cs.Msgs)
			if first != 1 || last != 8 {
				t.Fatalf("Unexpected first/last: %v/%v", first, last)
			}

			ms := cs.Msgs.(*FileMsgStore)
			ms.Lock()
			numSlices := len(ms.files)
			if numSlices != 3 {
				ms.Unlock()
				t.Fatalf("Expected 3 file slices, got %v", numSlices)
			}
			// The first slice will have 1, 2, [3, 4]
			// The second will have 5, 6, 7
			// THe third will have 8
			type firstLast struct {
				first, last uint64
			}
			expected := make(map[int]firstLast)
			expected[1] = firstLast{1, 4}
			expected[2] = firstLast{5, 7}
			expected[3] = firstLast{8, 8}
			for i := ms.firstFSlSeq; i <= ms.lastFSlSeq; i++ {
				sl := ms.files[i]
				first := sl.firstSeq
				last := sl.lastSeq
				if first != expected[i].first || last != expected[i].last {
					ms.Unlock()
					t.Fatalf("Expected first/last to be %v/%v for slice %d, got %v/%v",
						expected[i].first, expected[i].last, i, first, last)
				}
			}
			ms.Unlock()
		})
	}
}

func TestFSExpirationWithTruncatedNonLastSlice(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t, SliceConfig(3, 0, 0, ""), BufferSize(0))
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	payload := []byte("msg")
	for seq := uint64(1); seq <= 5; seq++ {
		storeMsg(t, cs, "foo", seq, payload)
	}

	// Truncate the end of first slice
	ms := cs.Msgs.(*FileMsgStore)
	ms.RLock()
	fslice := ms.files[1]
	fname := fslice.file.name
	ms.RUnlock()

	s.Close()

	datContent, err := os.ReadFile(fname)
	if err != nil {
		t.Fatalf("Error reading %v: %v", fname, err)
	}
	if err := os.WriteFile(fname, datContent[:len(datContent)-5], 0666); err != nil {
		t.Fatalf("Error writing file %v: %v", fname, err)
	}

	// Now re-open the store with a max age limit.
	// Since the first slice is corrupted, one of the message in that
	// slice will be removed.
	sl := testDefaultStoreLimits
	sl.MaxAge = 100 * time.Millisecond
	s, state := openDefaultFileStoreWithLimits(t, &sl, BufferSize(0), TruncateUnexpectedEOF(true))
	defer s.Close()

	time.Sleep(200 * time.Millisecond)

	cs = getRecoveredChannel(t, state, "foo")
	n, b := msgStoreState(t, cs.Msgs)
	if n != 0 || b != 0 {
		t.Fatalf("Expected no message, got %v/%v", n, b)
	}
}

func TestFSRecoverEmptyIndexMsgFile(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ms := c.Msgs
	storeMsg(t, c, "foo", 1, []byte("msg"))
	ms.(*FileMsgStore).RLock()
	datname := ms.(*FileMsgStore).writeSlice.file.name
	idxname := ms.(*FileMsgStore).writeSlice.idxFile.name
	ms.(*FileMsgStore).RUnlock()
	s.Close()

	// Truncate both files to just file header size
	if err := os.Truncate(datname, 4); err != nil {
		t.Fatalf("Error truncating %s: %v", datname, err)
	}
	if err := os.Truncate(idxname, 4); err != nil {
		t.Fatalf("Error truncating %s: %v", datname, err)
	}

	s, rs := openDefaultFileStore(t)
	defer s.Close()

	c = getRecoveredChannel(t, rs, "foo")
	if n, b := msgStoreState(t, c.Msgs); n != 0 || b != 0 {
		t.Fatalf("Unexpected state: %v - %v", n, b)
	}

	// Files were empty, store one messages
	msg := storeMsg(t, c, "foo", 1, []byte("msg"))
	s.Close()

	// Truncate only index file
	if err := os.Truncate(idxname, 4); err != nil {
		t.Fatalf("Error truncating %s: %v", datname, err)
	}
	// Recover and check that message is recovered (index reconstructed)
	s, rs = openDefaultFileStore(t)
	defer s.Close()
	c = getRecoveredChannel(t, rs, "foo")
	rm := msgStoreLookup(t, c.Msgs, msg.Sequence)
	if !reflect.DeepEqual(rm, msg) {
		t.Fatalf("Expected %v, got %v", msg, rm)
	}
}

func TestFSEnsureLastMsgAndIndexMatch(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	storeMsg(t, c, "foo", 1, []byte("msg1"))
	storeMsg(t, c, "foo", 2, []byte("msg2"))
	ms := c.Msgs.(*FileMsgStore)
	ms.RLock()
	fname := ms.files[1].file.name
	offset := ms.wOffset
	ms.RUnlock()
	storeMsg(t, c, "foo", 3, []byte("msg3"))

	s.Close()

	f, err := os.OpenFile(fname, os.O_RDWR, 0666)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	if err := f.Truncate(offset); err != nil {
		t.Fatalf("Error on truncate: %v", err)
	}
	f.Close()
	f, err = os.OpenFile(fname, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	m3 := &pb.MsgProto{Sequence: 3, Subject: "foo", Data: []byte("msg3_modified")}
	if _, _, err := writeRecord(f, nil, recNoType, m3, m3.Size(), crc32.IEEETable); err != nil {
		t.Fatalf("Error rewriting file: %v", err)
	}
	f.Close()

	s, rs := openDefaultFileStore(t)
	defer s.Close()
	c = getRecoveredChannel(t, rs, "foo")
	if _, err := c.Msgs.Lookup(3); err != nil {
		t.Fatalf("Error on lookup: %v", err)
	}
}

func TestFSEmptyRemovesAllMsgsFiles(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ms := c.Msgs
	storeMsg(t, c, "foo", 1, []byte("msg"))
	ms.(*FileMsgStore).RLock()
	datname := ms.(*FileMsgStore).writeSlice.file.name
	idxname := ms.(*FileMsgStore).writeSlice.idxFile.name
	ms.(*FileMsgStore).RUnlock()
	s.Close()

	os.Remove(datname)

	s, rs := openDefaultFileStore(t)
	defer s.Close()

	c = getRecoveredChannel(t, rs, "foo")
	if n, b := msgStoreState(t, c.Msgs); n != 0 || b != 0 {
		t.Fatalf("Unexpected state: %v - %v", n, b)
	}
	if err := c.Msgs.Empty(); err != nil {
		t.Fatalf("Error on empty: %v", err)
	}
	timeout := time.Now().Add(time.Second)
	for time.Now().Before(timeout) {
		// Make sure we have remove the index file too.
		if fi, err := os.Stat(idxname); err != nil && fi == nil {
			return
		}
	}
	t.Fatalf("Expected index file to be gone, it was not")
}

func TestFSGetSeqFromTimestamp(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t, SliceConfig(3, 0, 0, ""))
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")

	var msgs []*pb.MsgProto
	for i := 0; i < 7; i++ {
		if i > 0 {
			time.Sleep(50 * time.Millisecond)
		}
		msg := storeMsg(t, c, "foo", uint64(i+1), []byte("msg"))
		msgs = append(msgs, msg)
	}

	for i := 0; i < 2; i++ {
		seq := msgStoreGetSequenceFromTimestamp(t, c.Msgs, msgs[0].Timestamp-int64(time.Duration(time.Second)))
		if seq != 1 {
			t.Fatalf("Expected to get seq 1, got %v", seq)
		}
		seq = msgStoreGetSequenceFromTimestamp(t, c.Msgs, msgs[6].Timestamp+int64(time.Duration(time.Second)))
		if seq != 8 {
			t.Fatalf("Expected to get seq 8, got %v", seq)
		}
		seq = msgStoreGetSequenceFromTimestamp(t, c.Msgs, msgs[3].Timestamp+int64(10*time.Millisecond))
		if seq != 5 {
			t.Fatalf("Expected to get seq 5, got %v", seq)
		}
		seq = msgStoreGetSequenceFromTimestamp(t, c.Msgs, msgs[5].Timestamp)
		if seq != 6 {
			t.Fatalf("Expected to get seq 6, got %v", seq)
		}

		// Set firstMsg and lastMsg to nil and repeat those tests.
		if i == 0 {
			ms := getFileMsgStore(c.Msgs)
			ms.Lock()
			ms.firstMsg, ms.lastMsg = nil, nil
			ms.Unlock()
		}
	}
}

func TestFSNoPanicOnRemoveMsg(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	l := &captureErrAndFatalLogger{}

	limits := testDefaultStoreLimits
	limits.MaxMsgs = 5
	s, err := NewFileStore(l, testFSDefaultDatastore, &limits, BufferSize(0))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	cs := storeCreateChannel(t, s, "foo")
	seq := uint64(1)
	for i := 0; i < 5; i++ {
		storeMsg(t, cs, "foo", seq, []byte("hello"))
		seq++
	}

	ms := cs.Msgs.(*FileMsgStore)
	ms.Lock()
	idxFile := ms.files[1].idxFile
	ms.fm.closeLockedOrOpenedFile(idxFile)
	content, err := os.ReadFile(idxFile.name)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	copy(content[4:], []byte("xxx"))
	if err := os.WriteFile(idxFile.name, content, 0600); err != nil {
		t.Fatalf("Error writing file: %v", err)
	}
	ms.Unlock()

	storeMsg(t, cs, "foo", seq, []byte("hello"))

	gotIt := false
	l.Lock()
	for _, m := range l.msgs {
		if strings.Contains(m, "Unable to remove") {
			gotIt = true
			break
		}
	}
	l.Unlock()
	if !gotIt {
		t.Fatal("Did not get error about removing message")
	}
}

func TestFSReadBuffer(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	cacheTTL = int64(5 * time.Second)
	defer func() { cacheTTL = testFSDefaultCacheTTL }()

	limits := testDefaultStoreLimits
	s, err := NewFileStore(testLogger, testFSDefaultDatastore, &limits, BufferSize(0), ReadBufferSize(0))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	for i := uint64(1); i <= 10; i++ {
		storeMsg(t, c, "foo", i, []byte(fmt.Sprintf("%v", i)))
	}
	c.Msgs.Flush()

	// Force empty of cache
	ms := c.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.cache.empty()
	ms.Unlock()

	// Lookup first message and check that it is in the cache.
	m := msgStoreLookup(t, c.Msgs, 1)
	if string(m.Data) != "1" {
		t.Fatalf("Expected message's data to be %q, got %q", "1", m.Data)
	}
	// Ensure that other messages are not in the cache
	for i := uint64(2); i <= 10; i++ {
		ms.Lock()
		m := ms.cache.get(i)
		ms.Unlock()
		if m != nil {
			t.Fatalf("Expected msg seq %v to not be in the cache, got %v", i, m)
		}
	}

	s.Close()
	cleanupFSDatastore(t)

	// Restart test but now with a read buffer...
	s, err = NewFileStore(testLogger, testFSDefaultDatastore, &limits, BufferSize(0), ReadBufferSize(1024*1024))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	c = storeCreateChannel(t, s, "foo")
	for i := uint64(1); i <= 10; i++ {
		storeMsg(t, c, "foo", i, []byte(fmt.Sprintf("%v", i)))
	}
	c.Msgs.Flush()

	// Force empty of cache
	ms = c.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.cache.empty()
	ms.Unlock()

	// Lookup first message
	msgStoreLookup(t, c.Msgs, 1)
	// All others should have been added to the cache
	for i := uint64(1); i <= 10; i++ {
		ms.Lock()
		m := ms.cache.get(i)
		ms.Unlock()
		if m == nil {
			t.Fatalf("Expected msg seq %v to be in cache, it was not", i)
		}
		if string(m.Data) != fmt.Sprintf("%v", i) {
			t.Fatalf("Expected content to be %q, got %q", i, m.Data)
		}
	}

	// Empty cache
	ms.Lock()
	ms.cache.empty()
	ms.Unlock()

	// Lookup message 8, which should load 9 and 10 too
	msgStoreLookup(t, c.Msgs, 8)
	for i := uint64(8); i <= 10; i++ {
		ms.Lock()
		m := ms.cache.get(i)
		ms.Unlock()
		if m == nil {
			t.Fatalf("Expected msg seq %v to be in cache, it was not", i)
		}
		if string(m.Data) != fmt.Sprintf("%v", i) {
			t.Fatalf("Expected content to be %q, got %q", i, m.Data)
		}
	}

	// Now lookup message 1, ensure that cache has only 10 elements...
	msgStoreLookup(t, c.Msgs, 1)
	ms.Lock()
	sizeCache := 0
	for cur := ms.cache.head; cur != nil; cur = cur.next {
		sizeCache++
	}
	ms.Unlock()
	if sizeCache != 10 {
		t.Fatalf("Expected cache size to be 10, got %v", sizeCache)
	}

	s.Close()
	cleanupFSDatastore(t)

	// Restart test but now with a read buffer that is too small for a single message
	s, err = NewFileStore(testLogger, testFSDefaultDatastore, &limits, BufferSize(0), ReadBufferSize(500))
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	defer s.Close()

	payload := make([]byte, 600)
	c = storeCreateChannel(t, s, "foo")
	for i := uint64(1); i <= 2; i++ {
		storeMsg(t, c, "foo", i, payload)
	}
	c.Msgs.Flush()

	// Force empty of cache
	ms = c.Msgs.(*FileMsgStore)
	ms.Lock()
	ms.cache.empty()
	ms.Unlock()

	// Lookup first message
	msgStoreLookup(t, c.Msgs, 1)
	// Ensure that other message is not in the cache
	ms.Lock()
	m = ms.cache.get(2)
	ms.Unlock()
	if m != nil {
		t.Fatalf("Expected msg seq 2 to not be in the cache, got %v", m)
	}
}

func TestFSReadMsgRecord(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ms := c.Msgs.(*FileMsgStore)

	r := &testReader{}

	var err error

	buf := make([]byte, recordHeaderSize+5)
	var retBuf []byte

	// Reader returns an error
	errReturned := fmt.Errorf("Fake error")
	r.setErrToReturn(errReturned)
	retBuf, err = ms.readMsgRecord(r, buf, 1)
	if !strings.Contains(err.Error(), errReturned.Error()) {
		t.Fatalf("Expected error %v, got: %v", errReturned, err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}

	// Record not containing CRC
	_header := [4]byte{}
	header := _header[:]
	util.ByteOrder.PutUint32(header, 0)
	r.setErrToReturn(nil)
	r.setContent(header)
	retBuf, err = ms.readMsgRecord(r, buf, 1)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}

	// Wrong CRC
	b := make([]byte, recordHeaderSize+5)
	util.ByteOrder.PutUint32(b, 5)
	copy(b[recordHeaderSize:], []byte("hello"))
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, err = ms.readMsgRecord(r, buf, 5)
	if err == nil {
		t.Fatal("Expected error got none")
	}
	if !reflect.DeepEqual(retBuf, b) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}

	// Not asking for CRC should return ok
	r.setContent(b)
	ms.fstore.opts.DoCRC = false
	retBuf, err = ms.readMsgRecord(r, buf, 5)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be same as the one provided")
	}
	if string(retBuf[recordHeaderSize:recordHeaderSize+5]) != "hello" {
		t.Fatalf("Expected body to be \"hello\", got %q", string(retBuf[recordHeaderSize:recordHeaderSize+5]))
	}

	// Check that returned buffer has expanded as required
	ms.fstore.opts.DoCRC = true
	b = make([]byte, recordHeaderSize+10)
	payload := []byte("hellohello")
	util.ByteOrder.PutUint32(b, uint32(len(payload)))
	util.ByteOrder.PutUint32(b[4:recordHeaderSize], crc32.ChecksumIEEE(payload))
	copy(b[recordHeaderSize:], payload)
	r.setErrToReturn(nil)
	r.setContent(b)
	retBuf, err = ms.readMsgRecord(r, buf, 10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if reflect.DeepEqual(retBuf, buf) {
		t.Fatal("Expected returned buffer to be different than the one provided")
	}
	if string(retBuf[recordHeaderSize:recordHeaderSize+10]) != string(payload) {
		t.Fatalf("Expected body to be %q got %v", string(payload), string(retBuf[recordHeaderSize:recordHeaderSize+10]))
	}

	// Append zeros to the end of buffer
	for i := 0; i < recordHeaderSize+2; i++ {
		b = append(b, 0)
	}
	// Don't call setContent since this would reset the read position
	r.content = b
	if _, err := ms.readMsgRecord(r, buf, 2); err != errNeedRewind {
		t.Fatalf("Expected error %v, got %v", errNeedRewind, err)
	}

	// Check that error returned if size does not match
	b = b[:len(b)-2]
	r.setContent(b)
	if _, err := ms.readMsgRecord(r, buf, 8); err == nil || !strings.Contains(err.Error(), "expected size") {
		t.Fatalf("Expected error about wrong size, got %v", err)
	}
}

func TestFSGapsInSequenceWithoutFillAndExpiration(t *testing.T) {
	opts := testFSGetOptionsForGapsTests()
	// Add case with smaller slice
	opts = append(opts, testFSGapsOption{
		name: "SmallFileSlice",
		opt:  SliceConfig(3, 0, 0, ""),
	})
	for _, o := range opts {
		t.Run(o.name, func(t *testing.T) {
			fillGaps = false
			defer func() { fillGaps = true }()

			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			s := createDefaultFileStore(t, o.opt)
			defer s.Close()

			limits := testDefaultStoreLimits
			limits.MaxMsgs = 6
			s.SetLimits(&limits)

			c := storeCreateChannel(t, s, "foo")
			ms := c.Msgs.(*FileMsgStore)

			seqs := []uint64{1, 2, 5, 8, 9, 10}
			for _, seq := range seqs {
				storeMsg(t, c, "foo", seq, []byte(fmt.Sprintf("msg%d", seq)))
			}
			ms.Flush()
			ms.Lock()
			ms.cache.empty()
			ms.Unlock()

			for _, seq := range seqs {
				msg, err := ms.Lookup(seq)
				if err != nil {
					t.Fatalf("Error on lookup: %v", err)
				}
				if msg.Sequence != seq || string(msg.Data) != fmt.Sprintf("msg%d", seq) {
					t.Fatalf("Unexpected message for seq %v: %v", seq, msg)
				}
			}

			notfound := []uint64{3, 6, 7}
			for _, seq := range notfound {
				msg, err := ms.Lookup(seq)
				if err != nil || msg != nil {
					t.Fatalf("Unexpected result err=%v msg=%v", err, msg)
				}
			}

			// Add more messages to force 3 first to be removed,
			// and in the case of the small slice option, should
			// cause first slice to be removed.
			// Add some more gaps..
			storeMsg(t, c, "foo", 12, []byte("msg12"))
			storeMsg(t, c, "foo", 14, []byte("msg14"))
			storeMsg(t, c, "foo", 16, []byte("msg16"))

			first, last := msgStoreFirstAndLastSequence(t, ms)
			if first != 8 && last != 16 {
				t.Fatalf("Expected first to be 8 and last to be 16, got %v and %v", first, last)
			}

			expectFirsSliceToBe := func(t *testing.T, expected int) {
				ms.Lock()
				ffseq := ms.firstFSlSeq
				ms.Unlock()
				if ffseq != expected {
					t.Fatalf("First slice expected to be %v, got %v", expected, ffseq)
				}
			}
			if o.name == "SmallFileSlice" {
				expectFirsSliceToBe(t, 2)
			} else {
				expectFirsSliceToBe(t, 1)
			}

			storeMsg(t, c, "foo", 18, []byte("msg18"))
			storeMsg(t, c, "foo", 19, []byte("msg19"))
			storeMsg(t, c, "foo", 20, []byte("msg20"))

			// Force expiration..
			time.Sleep(50 * time.Millisecond)
			ms.Lock()
			ms.expireMsgs(time.Now().UnixNano(), int64(time.Millisecond))
			ms.Unlock()

			n, b, err := ms.State()
			if err != nil {
				t.Fatalf("Error getting state: %v", err)
			}
			if n != 0 || b != 0 {
				t.Fatalf("Unexpected number of msgs: %v bytes: %v", n, b)
			}

			if o.name == "SmallFileSlice" {
				expectFirsSliceToBe(t, 4)
			} else {
				expectFirsSliceToBe(t, 1)
			}
		})
	}
}

func TestFSExpirationError(t *testing.T) {
	cleanupFSDatastore(t)
	defer cleanupFSDatastore(t)

	s := createDefaultFileStore(t)
	defer s.Close()

	c := storeCreateChannel(t, s, "foo")
	ms := c.Msgs.(*FileMsgStore)

	storeMsg(t, c, "foo", 1, []byte("msg"))
	ms.Flush()
	ms.Lock()
	ms.cache.empty()
	ms.Unlock()

	ms.Lock()
	idxFile := ms.files[1].idxFile
	ms.fm.closeLockedOrOpenedFile(idxFile)
	content, err := os.ReadFile(idxFile.name)
	if err != nil {
		t.Fatalf("Error reading file: %v", err)
	}
	copy(content[4:], []byte("xxx"))
	if err := os.WriteFile(idxFile.name, content, 0600); err != nil {
		t.Fatalf("Error writing file: %v", err)
	}
	ms.Unlock()

	time.Sleep(50 * time.Millisecond)
	ms.Lock()
	now := time.Now().UnixNano()
	nextExpiration := ms.expireMsgs(time.Now().UnixNano(), int64(time.Millisecond))
	ms.Unlock()
	if nextExpiration < now+int64(4500*time.Millisecond) {
		t.Fatalf("Expected next expiration to be set to 5secs from now, got %v", time.Duration(nextExpiration))
	}
}

func TestFSMsgsFileVersionError(t *testing.T) {
	for _, test := range []struct {
		name   string
		dat    bool
		suffix string
	}{
		{"data", true, datSuffix},
		{"index", false, idxSuffix},
	} {
		t.Run(test.name, func(t *testing.T) {
			cleanupFSDatastore(t)
			defer cleanupFSDatastore(t)

			s := createDefaultFileStore(t)
			defer s.Close()

			c := storeCreateChannel(t, s, "foo")
			storeMsg(t, c, "foo", 1, []byte("hello"))
			if err := c.Msgs.Flush(); err != nil {
				t.Fatalf("Error flushing store: %v", err)
			}
			var fname string
			ms := c.Msgs.(*FileMsgStore)
			ms.Lock()
			if test.dat {
				fname = ms.files[1].file.name
			} else {
				fname = ms.files[1].idxFile.name
			}
			ms.Unlock()

			s.Close()

			os.Remove(fname)
			if err := os.WriteFile(fname, []byte(""), 0666); err != nil {
				t.Fatalf("Error writing file: %v", err)
			}

			s, err := NewFileStore(testLogger, testFSDefaultDatastore, nil)
			if err != nil {
				t.Fatalf("Error creating filestore: %v", err)
			}
			defer s.Close()
			if _, err := s.Recover(); err == nil ||
				!strings.Contains(err.Error(), "unable to recover message store for [foo]") ||
				!strings.Contains(err.Error(), fmt.Sprintf("%s1%s", msgFilesPrefix, test.suffix)) {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}
