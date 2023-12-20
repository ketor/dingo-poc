// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "butil/containers/flat_map.h"
#include "butil/string_printf.h"
#include "butil/synchronization/lock.h"
#include "common/helper.h"
#include "common/latch.h"

class DingoLatchTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST(DingoLatchTest, wakeup) {
  dingodb::Latches latches(1024);

  std::vector<std::string> keys_a{"k1", "k3", "k5"};
  std::vector<std::string> keys_b{"k4", "k5", "k6"};
  int64_t cid_a = 1;
  int64_t cid_b = 2;

  dingodb::Lock lock_a(keys_a);
  dingodb::Lock lock_b(keys_b);

  // a acquire lock success
  auto acquired_a = latches.Acquire(lock_a, cid_a);
  EXPECT_EQ(acquired_a, true);

  // b acquire lock failed
  auto acquired_b = latches.Acquire(lock_b, cid_b);
  EXPECT_EQ(acquired_b, false);

  // a release lock, and get wakeup list
  auto wakeup = latches.Release(lock_a, cid_a);
  EXPECT_EQ(wakeup[0], cid_b);

  // b acquire lock success
  acquired_b = latches.Acquire(lock_b, cid_b);
  EXPECT_EQ(acquired_b, true);
}

TEST(DingoLatchTest, wakeup_by_multi_cmds) {
  dingodb::Latches latches(256);

  std::vector<std::string> keys_a{"k1", "k2", "k3"};
  std::vector<std::string> keys_b{"k4", "k5", "k6"};
  std::vector<std::string> keys_c{"k3", "k4"};
  dingodb::Lock lock_a(keys_a);
  dingodb::Lock lock_b(keys_b);
  dingodb::Lock lock_c(keys_c);
  uint64_t cid_a = 1;
  uint64_t cid_b = 2;
  uint64_t cid_c = 3;

  // a acquire lock success
  auto acquired_a = latches.Acquire(lock_a, cid_a);
  EXPECT_EQ(acquired_a, true);

  // b acquire lock success
  auto acquired_b = latches.Acquire(lock_b, cid_b);
  EXPECT_EQ(acquired_b, true);

  // c acquire lock failed, cause a occupied slot 3
  auto acquired_c = latches.Acquire(lock_c, cid_c);
  EXPECT_EQ(acquired_c, false);

  // a release lock, and get wakeup list
  auto wakeup = latches.Release(lock_a, cid_a);
  EXPECT_EQ(wakeup[0], cid_c);

  // c acquire lock failed again, cause b occupied slot 4
  acquired_c = latches.Acquire(lock_c, cid_c);
  EXPECT_EQ(acquired_c, false);

  // b release lock, and get wakeup list
  wakeup = latches.Release(lock_b, cid_b);
  EXPECT_EQ(wakeup[0], cid_c);

  // finally c acquire lock success
  acquired_c = latches.Acquire(lock_c, cid_c);
  EXPECT_EQ(acquired_c, true);
}

TEST(DingoLatchTest, wakeup_by_small_latch_slot) {
  dingodb::Latches latches(5);

  std::vector<std::string> keys_a{"k1", "k2", "k3"};
  std::vector<std::string> keys_b{"k6", "k7", "k8"};
  std::vector<std::string> keys_c{"k3", "k4"};
  std::vector<std::string> keys_d{"k7", "k10"};
  dingodb::Lock lock_a(keys_a);
  dingodb::Lock lock_b(keys_b);
  dingodb::Lock lock_c(keys_c);
  dingodb::Lock lock_d(keys_d);
  u_int64_t cid_a = 1;
  u_int64_t cid_b = 2;
  u_int64_t cid_c = 3;
  u_int64_t cid_d = 4;

  auto acquired_a = latches.Acquire(lock_a, cid_a);
  EXPECT_EQ(acquired_a, true);

  // c acquire lock failed, cause a occupied slot 3
  auto acquired_c = latches.Acquire(lock_c, cid_c);
  EXPECT_EQ(acquired_c, false);

  // b acquire lock success
  auto acquired_b = latches.Acquire(lock_b, cid_b);
  EXPECT_EQ(acquired_b, true);

  // d acquire lock failed, cause a occupied slot 7
  auto acquired_d = latches.Acquire(lock_d, cid_d);
  EXPECT_EQ(acquired_d, false);

  // a release lock, and get wakeup list
  auto wakeup = latches.Release(lock_a, cid_a);
  EXPECT_EQ(wakeup[0], cid_c);

  // c acquire lock success
  acquired_c = latches.Acquire(lock_c, cid_c);
  EXPECT_EQ(acquired_c, true);

  // b release lock, and get wakeup list
  wakeup = latches.Release(lock_b, cid_b);
  EXPECT_EQ(wakeup[0], cid_d);

  // finally d acquire lock success
  acquired_d = latches.Acquire(lock_d, cid_d);
  EXPECT_EQ(acquired_d, true);
}