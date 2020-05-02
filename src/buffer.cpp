/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

  BufMgr::BufMgr(std::uint32_t bufs) :
      numBufs(bufs) {
    bufDescTable = new BufDesc[bufs];

    for (FrameId i = 0; i < bufs; i++) {
      bufDescTable[i].frameNo = i;
      bufDescTable[i].valid = false;
    }

    bufPool = new Page[bufs];

    int htsize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1; // point to the end
  }

  BufMgr::~BufMgr() {
    // TODO
  }

  void BufMgr::advanceClock() {
    clockHand = (clockHand + 1) % numBufs;
    // TODO
  }

  void BufMgr::allocBuf(FrameId &frame) {
    /*
     * Allocates a free frame using the clock algorithm;
     * if necessary, writing a dirty page back to disk. Throws     BufferExceededException if all buffer frames are pinned.
     This private method will get called by the
     readPage() and allocPage() methods described below.
     Make sure that if the buffer frame allocated
     has a valid page in it, you remove the appropriate entry from the hash table.
     */
    while (true) {
      this->advanceClock();
      BufDesc tempBufDesc = this->bufDescTable[this->clockHand];
      File *tempFile = tempBufDesc.file;
      PageId tempPageID = tempBufDesc.pageNo;
      FrameId tempFrameID = tempBufDesc.frameNo;
      if (tempBufDesc.valid == true) {
        if (tempBufDesc.refbit == true) {
          tempBufDesc.refbit = false;
          continue;
        }
        if (tempBufDesc.pinCnt > 0) {
          continue;
        }
        if (tempBufDesc.dirty == true) {
          Page tempPage = this->bufPool[tempFrameID];
          tempFile->writePage(tempPage);
          // TODO
        }
      }
      try {
        this->hashTable->remove(tempFile, tempPageID);
      } catch (const HashNotFoundException &e) {
        continue;
      }
    }
    // TODO
  }

  void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
    // TODO
  }

  void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
    // TODO
  }

  void BufMgr::flushFile(const File *file) {
    // TODO
  }

  void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
    Page newPage = file->allocatePage();
    pageNo = newPage.page_number();
    *page = newPage;
    // TODO
  }

  void BufMgr::disposePage(File *file, const PageId PageNo) {
    // TODO

  }

  void BufMgr::printSelf(void) {
    BufDesc *tmpbuf;
    int validFrames = 0;

    for (std::uint32_t i = 0; i < numBufs; i++) {
      tmpbuf = &(bufDescTable[i]);
      std::cout << "FrameNo:" << i << " ";
      tmpbuf->Print();

      if (tmpbuf->valid == true)
        validFrames++;
    }

    std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
  }

}
