#include <errno.h>
#include <fcntl.h>
#include <gflags/gflags.h>
#include <immintrin.h>
#include <libaio.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <csignal>
#include <exception>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <span>
#include <thread>
#include <vector>
#include <sstream>
#include <fstream>
#include <string>

#include "/users/nuoxu333/vmcache/util.h"
#include "/users/nuoxu333/vmcache/tpcc/RandomGenerator.hpp"
#include "db_treeline.h"

using namespace std;

struct DiskStats {
  std::string device;
  double tps;
  double avgReadsPerSecond;
  double avgWritesPerSecond;
  double dscdPerSecond;
  uint64_t totalKbRead;
  uint64_t totalKbWritten;
  uint64_t totalKbDscd;
};

DiskStats parseIOStatOutput(const std::string& output) {
  std::istringstream iss(output);
  string line;

  DiskStats stats;

  while (std::getline(iss, line)) {
      if(line.find("nvme4n1") != string::npos){
        istringstream lineStream(line);
        lineStream >> stats.device >> stats.tps >> stats.avgReadsPerSecond >> stats.avgWritesPerSecond >> stats.dscdPerSecond >> stats.totalKbRead >> stats.totalKbWritten >> stats.totalKbDscd;
        break;
      }
  }

  return stats;
}

// benchmark tuning parameters
DEFINE_int32(ops, 1000, "number of operations");
DEFINE_int32(key_size, 5, "key size");
DEFINE_int32(value_size, 10, "value size");
DEFINE_string(workload, "random_insert", "workload type");
DEFINE_int32(threads, 1, "number of threads");
DEFINE_bool(use_iterative_flush, false, "use iterative flush or recursive flush");
DEFINE_bool(no_read, false, "insertion only workload");
DEFINE_bool(show_snapshot, false, "show tree detail");
DEFINE_int32(prefill, 5000, "fill up before benchmarking");
DEFINE_int32(insertion_threads, 1, "number of insertion thread");

std::atomic<bool> keepRunning = {false};
std::atomic<uint64_t> curOps = {0};
std::atomic<uint64_t> totalCnt = {0};

PGTreeLineDB* tldb;

std::string gen_random(const int len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string tmp_s;
  tmp_s.reserve(len);

  for (int i = 0; i < len; ++i) {
    tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  return tmp_s;
}

inline void showProgress(int i, bool detailed = false) {
  int cond = 0;
  if (i < 1000) {
    cond = 100;
  } else if (i < 10000) {
    cond = 1000;
  } else if (i < 100000) {
    cond = 10000;
  } else if (i < 1000000) {
    cond = 100000;
  } else {
    cond = 100000;
  }
  if(detailed) std::cout << "\rops finished: " << i;
  else if (i % cond == 0 && i >= cond) std::cout << "\rops finished: " << i;
  fflush(stdout);
}

void randomInsertion(std::vector<std::pair<std::string, std::string>>& test) {
  std::cout << "run random insertion" << std::endl;
  // generate testing data
  for(int i = 0; i < FLAGS_ops; ++i) {
    // build key
    // std::string key_str = gen_random(FLAGS_key_size);
    std::string value_str = gen_random(FLAGS_value_size);
    test.push_back(std::make_pair("", value_str));
  }
  std::cout << "done generating data" << std::endl;

  if(FLAGS_insertion_threads < FLAGS_threads) FLAGS_insertion_threads = FLAGS_threads;

  auto start = std::chrono::high_resolution_clock::now();

  keepRunning.store(true);
  parallel_for(0, FLAGS_ops - FLAGS_prefill, FLAGS_insertion_threads,
              [&](uint64_t worker, uint64_t begin, uint64_t end) {
                for (u64 i = begin + 1; i <= end; i++) {
                  // showProgress(i);
                  tl::Status s = tldb->Insert(i, test[i].second.c_str(), test[i].second.length());
                  if(!s.ok()) {
                    std::cout << "Failed to insert: " << s.ToString() << std::endl;
                    return;
                  }
                  curOps.fetch_add(1);
                  totalCnt.fetch_add(1);
                }
              });
  std::cout << "done insertion" << std::endl;
  keepRunning.store(false);
  auto end = std::chrono::high_resolution_clock::now();
  long long duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  float throughput = ((float)FLAGS_ops / duration);
  std::cout << "write throughput: " << throughput << " ops/μs" << std::endl;
} 

void workloadInsertion(std::vector<std::pair<std::string, std::string>>& test) {
  test.reserve(FLAGS_ops);
  if (FLAGS_workload == "random_insert") {
    randomInsertion(test);
  } else if (FLAGS_workload == "sequential_insert") {
    // sequentialInsertion(test);
  } else {
    std::cout << "workload not supported" << std::endl;
    exit(1);
  }
}

void WorkloadLookup(const std::vector<tl::pg::Record>& records) {
  auto start = std::chrono::high_resolution_clock::now();
  {
    keepRunning.store(true);
    parallel_for(0, FLAGS_prefill, FLAGS_threads,
              [&](uint64_t worker, uint64_t begin, uint64_t end) {
                for (u64 i = begin + 1; i <= end; i++) {
                  std::string value_out;
                  tl::Status s = tldb->Read(records[i].first, &value_out);
                  if(!s.ok()) {
                    std::cout << "Failed to lookup: " << s.ToString() << std::endl;
                    return;
                  }
                  curOps.fetch_add(1);
                  totalCnt.fetch_add(1);
                }
              });
    keepRunning.store(false);
  }
  auto end = std::chrono::high_resolution_clock::now();
  long long duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();
  float throughput = ((float)FLAGS_ops / duration);
  std::cout << "read throughput: " << throughput << " ops/μs" << std::endl;
}

void WorkloadInsertion() {
  std::vector<std::pair<std::string, std::string>> keys;
  workloadInsertion(keys);
}

void WorkloadUpdateWrapper(std::vector<tl::pg::Record>& records) {
  // generate new value for update, keep same length for now.
  // for(int i = 0; i < records.size(); ++i) {
  //   std::string new_val = gen_random(FLAGS_value_size);
  //   records[i].second = tl::Slice(new_val.c_str(), new_val.length());
  // }
  
  auto start = std::chrono::high_resolution_clock::now();
  keepRunning.store(true);
  parallel_for(0, FLAGS_ops, FLAGS_threads,
            [&](uint64_t worker, uint64_t begin, uint64_t end) {
              for(int i = begin + 1; i <= end; ++i) {
                tl::Status s = tldb->Update(records[i].first, records[i].second.data(), records[i].second.size());
                if(!s.ok()) {
                  std::cout << "Failed to update: " << s.ToString() << std::endl;
                  return;
                }
                totalCnt.fetch_add(1);
                curOps.fetch_add(1);
              }                
            });
  keepRunning.store(false);
  auto end = std::chrono::high_resolution_clock::now();
  long long duration =
    std::chrono::duration_cast<std::chrono::microseconds>(end - start)
        .count();
  float throughput = ((float)FLAGS_ops / duration);
  std::cout << "update throughput: " << throughput << " ops/μs" << std::endl;
}

void WorkloadReadModifyWrite(std::vector<tl::pg::Record>& records) {
  // generate new value for update, keep same length for now.
  // for(int i = 0; i < keys.size(); ++i) {
  //   keys[i].second = gen_random(FLAGS_value_size);
  // }
  auto start = std::chrono::high_resolution_clock::now();
  keepRunning.store(true);
  parallel_for(0, FLAGS_threads, FLAGS_threads,
            [&](uint64_t worker, uint64_t begin, uint64_t end) {
              while(totalCnt.load() < FLAGS_ops) {
                u64 idx = RandomGenerator::getRand<u64>(0, FLAGS_prefill);
                // get key
                uint64_t key = records[idx].first;

                // read-modify-write
                {
                  std::string value_out;
                  tl::Status s = tldb->Read(key, &value_out);
                  if(!s.ok()) {
                    std::cout << "Failed to read: " << s.ToString() << std::endl;
                    return;
                  }

                  s = tldb->Update(key, records[idx].second.data(), records[idx].second.size());
                  if(!s.ok()) {
                    std::cout << "Failed to read: " << s.ToString() << std::endl;
                    return;
                  }
                }

                totalCnt.fetch_add(1);
                curOps.fetch_add(1);
              }
            });
  keepRunning.store(false);
  auto end = std::chrono::high_resolution_clock::now();
  long long duration =
    std::chrono::duration_cast<std::chrono::microseconds>(end - start)
        .count();
  float throughput = ((float)FLAGS_ops / duration);
  std::cout << "rmw throughput: " << throughput << " ops/μs" << std::endl;
}

void Run(std::vector<tl::pg::Record>& records) {
  if(FLAGS_workload == "update") {
    WorkloadUpdateWrapper(records);
  } else if(FLAGS_workload == "rmw") {
    WorkloadReadModifyWrite(records);
  } else if(FLAGS_workload == "random_lookup") {
    std::cout << "> " << FLAGS_prefill << " lookups on " << FLAGS_prefill << " key-value pairs" << std::endl;
    WorkloadLookup(records);
  } else {
    std::cout << "> " << FLAGS_ops - FLAGS_prefill << " insertions after " << FLAGS_prefill << " key-value pairs bulkloaded" << std::endl;
    WorkloadInsertion();
  }
}

tl::Status BulkLoad(uint64_t keynum, std::vector<tl::pg::Record>& records) {
  records.reserve(FLAGS_ops);
  for (uint64_t key = 1; key < (keynum + 1); ++key) {
    const tl::key_utils::IntKeyAsSlice strkey(key);
    std::string rndValue = gen_random(FLAGS_value_size);
    records.emplace_back(key, tl::Slice(rndValue.c_str(), rndValue.length()));
  }
  return tldb->BulkLoad(records);
}

DiskStats getDiskStat() {
  string command = "iostat -d -k 1 1 | grep nvme4n1"; // Run iostat command
  string output;
  
  // Execute command and capture output
  FILE* pipe = popen(command.c_str(), "r");
  if (!pipe) {
      cerr << "Error executing iostat command" << endl;
      return DiskStats{};
  }
  char buffer[128];
  while (!feof(pipe)) {
      if (fgets(buffer, 128, pipe) != NULL) {
          output += buffer;
      }
  }
  pclose(pipe);

  // Parse output
  DiskStats stats = parseIOStatOutput(output);
  return stats;
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  FLAGS_record_size_bytes = 8 + FLAGS_value_size;

  auto statFn = [&]() {
    u64 cnt = 0;
    while(!keepRunning.load());
    cout << "ts,rmb,wmb,ops" << endl;
    while(keepRunning.load()) {
      sleep(1);
      float rmb = 0;
      float wmb = 0;
      int ops = curOps;
      cout << cnt++ << "," << rmb << "," << wmb << "," << ops << endl;
    }
  };

  tldb = new PGTreeLineDB();
  tldb->InitializeDatabase("/mnt/nvme/pgtl");
  tldb->SetKeyDistHints(0, 200000000, FLAGS_ops);

  std::vector<tl::pg::Record> records;
  // bulk load first
  tl::Status s = BulkLoad(FLAGS_prefill, records);
  if(!s.ok()) {
    std::cout << s.ToString() << std::endl;
    return 0;
  }
  std::cout << "Bulk load done, run workload" << std::endl;

  thread statThread(statFn);

  DiskStats startStat = getDiskStat();
  Run(records);
  DiskStats endStat = getDiskStat();

  std::cout << "Total MB read from disk: " << (endStat.totalKbRead - startStat.totalKbRead) / 1024 << std::endl;
  std::cout << "Total MB written to disk: " << (endStat.totalKbWritten - startStat.totalKbWritten) / 1024 << std::endl;
  tldb->ShutdownDatabase();
  delete tldb;
  statThread.join();
}