//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include <iostream>
#include <pthread.h>
#include <string>
#include <chrono>
#include <vector>
#include <tuple>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"

namespace ycsbc {

inline std::tuple<long long, std::vector<int>> ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim, ThreadPool *threadpool, 
                        int client_id, int target_ops_per_s, int burst_gap_ms, int burst_size_ops, uint64_t first_burst_ops, bool forced_warmup,
                        int num_read_burst_cycles, size_t read_burst_num_records) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);

  size_t cpu_for_client = client_id + 8;
  CPU_SET(cpu_for_client, &cpuset);
  std::cout << "[TGRIGGS_LOG] Pinning client to " << cpu_for_client << std::endl;
  // int rc = pthread_setaffinity_np(pthread_self(),
  //                                 sizeof(cpu_set_t), &cpuset);
  // if (rc != 0) {
  //   fprintf(stderr, "Couldn't set thread affinity.\n");
  //   std::exit(1);
  // }

  int num_bursts = burst_size_ops;
  if (burst_size_ops == 0) {
    num_bursts = 1;
  }
  int adjusted_num_ops = num_ops;
 
  std::vector<int> op_progress;       
  int client_log_interval_s = 1;                 

  long target_ops_tick_ns = 0;
  if (target_ops_per_s > 0) {
    target_ops_tick_ns = (long) (1000000000 / target_ops_per_s);
  }

  try {
    if (init_db) {
      db->Init();
    }

    auto client_start = std::chrono::system_clock::now();
    auto client_start_micros = std::chrono::duration_cast<std::chrono::microseconds>(client_start.time_since_epoch()).count();
    auto interval_start_time = std::chrono::steady_clock::now();

    std::cout << "[FAIRDB Log] Client starting at " << std::to_string(client_start_micros) << " for " << std::to_string(num_ops) << " ops" << std::endl;

    if (forced_warmup) {
      wl->DoWarmup(*db, client_id, target_ops_tick_ns, target_ops_per_s, threadpool);
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    int ops = 0;
    for (int b = 0; b < num_bursts; ++b) {
      if (num_bursts == 2 && first_burst_ops != 0) {
        if (b == 0) {
          adjusted_num_ops = first_burst_ops;
        } else {
          adjusted_num_ops = num_ops - first_burst_ops;
        }
      }

      if (num_read_burst_cycles == 0) {
        num_read_burst_cycles = 1;
        read_burst_num_records = 0;
      }

      for (int read_burst_cycle_i = 0; read_burst_cycle_i < num_read_burst_cycles; read_burst_cycle_i ++) {

        for (int i = 0; i < adjusted_num_ops; ++i) {
          if (rlim) {
            rlim->Consume(1);
          }

          auto op_start_time = std::chrono::high_resolution_clock::now();
          auto op_start_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(op_start_time.time_since_epoch()).count();

          if (is_loading) {
            wl->DoInsert(*db);
          } else {
            auto txn_lambda = [wl, db, client_id]() {
              wl->DoTransaction(*db, client_id);
              return nullptr;
            };
            
            // Submit operation and do not wait for a return. 
            threadpool->async_dispatch(client_id, txn_lambda);
          }
          ops++;

          // Periodically check whether log interval has been hit
          if (i % 100 == 0) {
            auto current_time = std::chrono::steady_clock::now();
            auto elapsedTime = std::chrono::duration_cast<std::chrono::seconds>(current_time - interval_start_time);
            if (elapsedTime.count() >= client_log_interval_s) {
              op_progress.push_back(ops);
              interval_start_time = std::chrono::steady_clock::now();
            }
            // auto current_time_sys = std::chrono::system_clock::now();
            // auto total_exp_time = std::chrono::duration_cast<std::chrono::seconds>(current_time_sys - client_start);
            // if (total_exp_time.count() >= total_exp_duration_s) {
            //   break;
            // }
          }
          
          EnforceClientRateLimit(op_start_time_ns, target_ops_per_s, target_ops_tick_ns, ops);
        }

        if (read_burst_num_records > 0) {
          auto txn_lambda = [wl, db, client_id, read_burst_num_records]() {
            wl->ReadBurstRecords(*db, client_id, read_burst_num_records);
            return nullptr;
          };
          
          // Submit operation and do not wait for a return. 
          threadpool->async_dispatch(client_id, txn_lambda);
        }
      }
      
      std::this_thread::sleep_for(std::chrono::milliseconds(burst_gap_ms));
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    op_progress.push_back(ops);
    return std::make_tuple(client_start_micros, op_progress);
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
