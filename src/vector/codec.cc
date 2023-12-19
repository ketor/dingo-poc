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

#include "vector/codec.h"

#include <cstdint>
#include <string>
#include <utility>

#include "butil/compiler_specific.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {

void VectorCodec::EncodeVectorRawKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result) {
  std::string user_result;
  EncodeVectorUserKey(prefix, partition_id, vector_id, user_result);
  result = Helper::PaddingUserKey(user_result);
}

void VectorCodec::EncodeVectorUserKey(char prefix, int64_t partition_id, int64_t vector_id, std::string& result) {
  if (BAIDU_UNLIKELY(prefix == 0)) {
    // Buf buf(16);
    // Buf buf(Constant::kVectorKeyMaxLen);
    // buf.WriteLong(partition_id);
    // DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);
    // buf.GetBytes(result);

    // prefix == 0 is not allowed
    DINGO_LOG(FATAL) << "Encode vector key failed, prefix is 0, partition_id:[" << partition_id << "], vector_id:["
                     << vector_id << "]";
  }

  // Buf buf(17);
  Buf buf(Constant::kVectorKeyMaxLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, vector_id);

  buf.GetBytes(result);
}

int64_t VectorCodec::DecodeVectorIdFromRawKey(const std::string& raw_key) {
  CHECK(!raw_key.empty());
  std::string user_key = Helper::DecodeRawKey(raw_key);
  if (user_key.empty()) {
    DINGO_LOG(ERROR) << "Decode vector id failed, value is empty, raw value:[" << Helper::StringToHex(raw_key) << "]";
    return 0;
  }

  return DecodeVectorIdFromUserKey(user_key);
}

int64_t VectorCodec::DecodeVectorIdFromUserKey(const std::string& user_key) {
  Buf buf(user_key);
  if (user_key.size() == Constant::kVectorKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (user_key.size() == Constant::kVectorKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(FATAL) << "Decode vector id failed, value size is not 9 or 17, value:[" << Helper::StringToHex(user_key)
                     << "]";
    return 0;
  }

  // return buf.ReadLong();
  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

int64_t VectorCodec::DecodePartitionIdFromRawKey(const std::string& raw_key) {
  CHECK(!raw_key.empty());
  std::string user_key = Helper::DecodeRawKey(raw_key);

  return DecodePartitionIdFromUserKey(user_key);
}

int64_t VectorCodec::DecodePartitionIdFromUserKey(const std::string& user_key) {
  Buf buf(user_key);

  // if (value.size() == 17 || value.size() == 9) {
  if (user_key.size() == Constant::kVectorKeyMaxLenWithPrefix ||
      user_key.size() == Constant::kVectorKeyMinLenWithPrefix) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

std::string VectorCodec::DecodeRawKeyToString(const std::string& raw_key) {
  return fmt::format("{}_{}", DecodePartitionIdFromRawKey(raw_key), DecodeVectorIdFromRawKey(raw_key));
}

std::string VectorCodec::DecodeUserKeyToString(const std::string& user_key) {
  return fmt::format("{}_{}", DecodePartitionIdFromUserKey(user_key), DecodeVectorIdFromUserKey(user_key));
}

std::string VectorCodec::DecodeRawRangeToString(const pb::common::Range& raw_range) {
  return fmt::format("[{}, {})", DecodeRawKeyToString(raw_range.start_key()),
                     DecodeRawKeyToString(raw_range.end_key()));
}

std::string VectorCodec::DecodeUserRangeToString(const pb::common::Range& user_range) {
  return fmt::format("[{}, {})", DecodeUserKeyToString(user_range.start_key()),
                     DecodeUserKeyToString(user_range.end_key()));
}

void VectorCodec::DecodeUserRangeToVectorId(const pb::common::Range& user_range, int64_t& begin_vector_id,
                                            int64_t& end_vector_id) {
  begin_vector_id = VectorCodec::DecodeVectorIdFromUserKey(user_range.start_key());
  int64_t temp_end_vector_id = VectorCodec::DecodeVectorIdFromUserKey(user_range.end_key());
  if (temp_end_vector_id > 0) {
    end_vector_id = temp_end_vector_id;
  } else {
    if (DecodePartitionIdFromUserKey(user_range.end_key()) > DecodePartitionIdFromUserKey(user_range.start_key())) {
      end_vector_id = INT64_MAX;
    }
  }
}

bool VectorCodec::IsValidUserKey(const std::string& user_key) {
  // return (key.size() == 8 || key.size() == 9 || key.size() == 16 || key.size() == 17);
  return (user_key.size() == Constant::kVectorKeyMinLenWithPrefix ||
          user_key.size() == Constant::kVectorKeyMaxLenWithPrefix);
}

}  // namespace dingodb