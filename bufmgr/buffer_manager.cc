#include "buffer_manager.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/statvfs.h>

#include <mutex>

#include "file_manager.h"
#include "twoqueue_eviction.h"

namespace llsm {

// Initializes a BufferManager to keep up to `options.buffer_pool_size /
// Page::kSize` frames in main memory. Bypasses file system cache if
// `options.use_direct_io` is true.
BufferManager::BufferManager(const BufMgrOptions& options,
                             std::filesystem::path db_path)
    : buffer_manager_size_(options.buffer_pool_size / Page::kSize) {
  size_t alignment = BufferManager::kDefaultAlignment;
  struct statvfs fs_stats;
  if (statvfs(db_path.c_str(), &fs_stats) == 0) {
    alignment = fs_stats.f_bsize;
  }
  // Allocate space with alignment because of O_DIRECT requirements.
  // No need to zero out because data brought here will come from the database
  // `File`, which is zeroed out upon expansion.
  pages_cache_ = aligned_alloc(alignment, options.buffer_pool_size);

  page_to_frame_map_ = std::make_unique<SyncHashTable<uint64_t, BufferFrame*>>(
      buffer_manager_size_, /*num_partitions = */ 1);
  frames_ = std::vector<BufferFrame>(buffer_manager_size_);
  free_ptr_ = 0;

  page_eviction_strategy_ =
      std::make_unique<TwoQueueEviction>(buffer_manager_size_);
  file_manager_ = std::make_unique<FileManager>(options, std::move(db_path));
}

// Writes all dirty pages back and frees resources.
BufferManager::~BufferManager() {
  LockMapMutex();
  LockEvictionMutex();

  // Write all the dirty pages back.
  BufferFrame* frame;
  while ((frame = page_eviction_strategy_->Evict()) != nullptr) {
    frame->Lock(/*exclusive=*/false);
    if (frame->IsDirty()) WritePageOut(frame);
    frame->Unlock();
  }

  free(pages_cache_);

  UnlockEvictionMutex();
  UnlockMapMutex();
}

// Retrieves the page given by `page_id`, to be held exclusively or not
// based on the value of `exclusive`. Pages are stored on disk in files with
// the same name as the page ID (e.g. 1).
BufferFrame& BufferManager::FixPage(const uint64_t page_id,
                                    const bool exclusive) {
  BufferFrame* frame = nullptr;
  size_t frame_id;
  bool success;

  // Check if page is already loaded in some frame.
  LockMapMutex();
  LockEvictionMutex();
  auto frame_lookup = page_to_frame_map_->UnsafeLookup(page_id, &frame);

  // If yes, load the corresponding frame
  if (frame_lookup) {
    if (frame->IncFixCount() == 1) page_eviction_strategy_->Delete(frame);
    UnlockEvictionMutex();
    UnlockMapMutex();

  } else {  // If not, we have to bring it in from disk.
    UnlockEvictionMutex();
    UnlockMapMutex();

    success = CreateFrame(page_id, &frame_id);

    if (!success) {  // Must evict something to make space.
      // Block here until you can evict something
      while (frame == nullptr) {
        LockMapMutex();
        LockEvictionMutex();
        frame = page_eviction_strategy_->Evict();
        if (frame != nullptr) {
          page_to_frame_map_->UnsafeErase(frame->GetPageId());
        }
        UnlockEvictionMutex();
        UnlockMapMutex();
      }

      // Write out evicted page if necessary
      if (frame->IsDirty()) {
        WritePageOut(frame);
        frame->UnsetDirty();
      }

      // Reset frame to new page_id
      ResetFrame(frame, page_id);
    } else {
      frame = &frames_[frame_id];
      frame->Initialize(page_id, FrameIdToData(frame_id));
    }

    // Read the page from disk into the selected frame.
    frame->IncFixCount();
    ReadPageIn(frame);

    // Insert the frame into the map.
    LockMapMutex();
    page_to_frame_map_->UnsafeInsert(page_id, frame);
    UnlockMapMutex();
  }

  frame->Lock(exclusive);

  return *frame;
}

// Unfixes a page updating whether it is dirty or not.
void BufferManager::UnfixPage(BufferFrame& frame, const bool is_dirty) {
  if (is_dirty) frame.SetDirty();

  // Since this page is now unfixed, check if it can be considered for eviction.
  LockEvictionMutex();
  if (frame.DecFixCount() == 0) page_eviction_strategy_->Insert(&frame);
  UnlockEvictionMutex();

  frame.Unlock();
}

// Flushes a page to disk and then unfixes it (the page is not necessarily
// immediately evicted from the cache).
void BufferManager::FlushAndUnfixPage(BufferFrame& frame) {
  WritePageOut(&frame);
  UnfixPage(frame, /*is_dirty=*/false);
}

// Writes all dirty pages to disk (without unfixing)
void BufferManager::FlushDirty() {
  LockMapMutex();

  for (auto& frame : frames_) {
    if (frame.IsDirty()) {
      frame.Lock(/*exclusive=*/false);
      WritePageOut(&frame);
      frame.UnsetDirty();
      frame.Unlock();
    }
  }

  UnlockMapMutex();
}

// Writes the page held by `frame` to disk.
void BufferManager::WritePageOut(BufferFrame* frame) const {
  file_manager_->WritePage(frame->GetPageId(), frame->GetData());
}

// Reads a page from disk into `frame`.
void BufferManager::ReadPageIn(BufferFrame* frame) {
  file_manager_->ReadPage(frame->GetPageId(), frame->GetData());
}

// Creates a new frame and specifies that it will hold the page with `page_id`.
// Returns nullptr if no new frame can be created.
bool BufferManager::CreateFrame(const uint64_t page_id, size_t* frame_id) {
  size_t frame_id_loc = PostIncFreePtr();
  if (frame_id_loc >= buffer_manager_size_) {
    return false;
  } else {
    *frame_id = frame_id_loc;
    return true;
  }
}

// Resets an exisiting frame to hold the page with `new_page_id`.
void BufferManager::ResetFrame(BufferFrame* frame, const uint64_t new_page_id) {
  frame->UnsetAllFlags();
  frame->SetPageId(new_page_id);
  frame->ClearFixCount();
}

size_t BufferManager::PostIncFreePtr() {
  const size_t old_val = free_ptr_++;
  if (old_val > buffer_manager_size_) free_ptr_ = buffer_manager_size_;
  return old_val;
}

void* BufferManager::FrameIdToData(const uint64_t frame_id) const {
  char* page_ptr = reinterpret_cast<char*>(pages_cache_);
  page_ptr += (frame_id * Page::kSize);

  return reinterpret_cast<void*>(page_ptr);
}

}  // namespace llsm
