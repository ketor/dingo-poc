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

#ifndef DINGODB_COMMON_LATCH_H_
#define DINGODB_COMMON_LATCH_H_

#include <cassert>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <vector>

namespace dingodb {

class Latch {
 public:
  std::deque<std::pair<uint64_t, uint64_t>> waiting{};

  Latch() = default;

  std::optional<uint64_t> GetFirstReqByHash(uint64_t hash);

  std::optional<std::pair<uint64_t, uint64_t>> PopFront(uint64_t key_hash);

  void WaitForWake(uint64_t key_hash, uint64_t cid);

  void PushPreemptive(uint64_t key_hash, uint64_t cid);

 private:
  void MaybeShrink();
};

class Lock {
 public:
  std::vector<uint64_t> requiredHashes{};
  size_t ownedCount = 0;

  Lock(const std::vector<std::string>& keys);

  bool Acquired() const;

  void ForceAssumeAcquired();

  bool IsWriteLock() const;

  static uint64_t Hash(const std::string& key);
};

class Latches {
 public:
  explicit Latches(size_t size);
  ~Latches();

  bool Acquire(Lock& lock, uint64_t who);

  std::vector<uint64_t> Release(const Lock& lock, uint64_t who);

 private:
  struct Slot {
    std::mutex mutex;
    Latch latch;
  };

  std::vector<Slot>* slots_;
  size_t size_;

  static size_t NextPowerOfTwo(size_t n);

  size_t GetSlotIndex(uint64_t hash) const;
};

}  // namespace dingodb

#endif