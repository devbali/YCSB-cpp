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
#include <string>

#include "db.h"
#include "core_workload.h"
#include "utils/countdown_latch.h"
#include "utils/rate_limit.h"
#include "utils/utils.h"
#include "fair_scheduler.h"
#include "threadpool.h"

namespace ycsbc {

inline int ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops, bool is_loading,
                        bool init_db, bool cleanup_db, utils::CountDownLatch *latch, utils::RateLimiter *rlim,
                        ThreadPool *threadpool) {
                        // FairScheduler *scheduler) {

  try {
    if (init_db) {
      db->Init();
    }

    int ops = 0;
    for (int i = 0; i < num_ops; ++i) {
      if (rlim) {
        rlim->Consume(1);
      }

      if (is_loading) {
        wl->DoInsert(*db);
      } else {

        // return thread_pool_.accessDB([this, &table, &key, fields, &result] {
        //     return ReadSingle(table, key, fields, result);
        // });

        auto txn_lambda = [wl, db]() {
          wl->DoTransaction(*db);
          return nullptr;  // to match void* return
        };
        threadpool->dispatch(txn_lambda);

        // scheduler->Schedule([wl, db] {
        //   return wl->DoTransaction(*db);
        // });

        // wl->DoTransaction(*db);
      }
      ops++;
    }

    if (cleanup_db) {
      db->Cleanup();
    }

    latch->CountDown();
    return ops;
  } catch (const utils::Exception &e) {
    std::cerr << "Caught exception: " << e.what() << std::endl;
    exit(1);
  }
}

} // ycsbc

#endif // YCSB_C_CLIENT_H_
