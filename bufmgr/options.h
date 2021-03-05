#pragma once

#include <cstdlib>

#include "db/page.h"
#include "llsm/options.h"

namespace llsm {

// Configuration options used by the `BufferManager`.
struct BufMgrOptions {
  // Create `BufMgrOptions` using its default values (defined below).
  BufMgrOptions() {}

  // Sets the options that are identical in both `Options` and `BufMgrOptions`
  // using an existing `Options` instance. All other options are set to their
  // default values (defined below).
  explicit BufMgrOptions(const Options& options)
      : buffer_pool_size(options.buffer_pool_size),
        use_direct_io(options.use_direct_io) {}

  // Sets `num_pages` based on the hints provided in `KeyDistHints`.
  BufMgrOptions& SetNumPagesUsing(const KeyDistHints& key_hints) {
    num_pages = key_hints.num_keys / key_hints.records_per_page();
    if (key_hints.num_keys % key_hints.records_per_page() != 0)
      ++num_pages;
    return *this;
  }

  // The size of the buffer pool, in bytes.
  size_t buffer_pool_size = 64 * 1024 * 1024;

  // The total number of pages that exist in the database.
  size_t num_pages = 1;

  // The number of segments to use to store the pages. The pages are equally
  // divided among all the segments.
  size_t num_segments = 1;

  // Whether or not the buffer manager should use direct I/O.
  bool use_direct_io = false;
};

}  // namespace llsm