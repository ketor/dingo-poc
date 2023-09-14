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

#include "server/store_service.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/constant.h"
#include "common/context.h"
#include "common/failpoint.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "fmt/core.h"
#include "meta/store_meta_manager.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "proto/error.pb.h"
#include "proto/node.pb.h"
#include "proto/store.pb.h"
#include "server/server.h"
#include "server/service_helper.h"

using dingodb::pb::error::Errno;

namespace dingodb {

DECLARE_uint32(max_prewrite_count);

StoreServiceImpl::StoreServiceImpl() = default;

butil::Status ValidateKvGetRequest(const dingodb::pb::store::KvGetRequest* request) {
  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvGet(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvGetRequest* request,
                             dingodb::pb::store::KvGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::KvGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->KvGet(ctx, keys, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }
  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }

  DINGO_LOG(DEBUG) << fmt::format("KvGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvBatchGetRequest(const dingodb::pb::store::KvBatchGetRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(key);
  }

  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchGet(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchGetRequest* request, pb::store::KvBatchGetResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  if (request->keys().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvBatchGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchGetRequest*>(request);
  status = storage_->KvGet(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()), kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvBatchGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  Helper::VectorToPbRepeated(kvs, response->mutable_kvs());

  DINGO_LOG(DEBUG) << fmt::format("KvBatchGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvPutRequest(const dingodb::pb::store::KvPutRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateSystemCapacity();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvPut(google::protobuf::RpcController* controller,
                             const dingodb::pb::store::KvPutRequest* request,
                             dingodb::pb::store::KvPutResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvPut request: " << request->ShortDebugString();

  butil::Status status = ValidateKvPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::store::KvPutRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPut(ctx, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
  }
}

butil::Status ValidateKvBatchPutRequest(const dingodb::pb::store::KvBatchPutRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(kv.key());
  }

  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateSystemCapacity();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchPut(google::protobuf::RpcController* controller,
                                  const pb::store::KvBatchPutRequest* request, pb::store::KvBatchPutResponse* response,
                                  google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvBatchPut request: " << request->ShortDebugString();
  if (request->kvs().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchPutRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvBatchPut request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutRequest*>(request);
  status = storage_->KvPut(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchPut request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvPutIfAbsentRequest(const dingodb::pb::store::KvPutIfAbsentRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateSystemCapacity();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvPutIfAbsent(google::protobuf::RpcController* controller,
                                     const pb::store::KvPutIfAbsentRequest* request,
                                     pb::store::KvPutIfAbsentResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);
  DINGO_LOG(DEBUG) << "KvPutIfAbsent request: " << request->ShortDebugString();

  butil::Status status = ValidateKvPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvPutIfAbsentRequest*>(request);
  std::vector<pb::common::KeyValue> kvs;
  kvs.emplace_back(std::move(*mut_request->release_kv()));
  status = storage_->KvPutIfAbsent(ctx, kvs, true);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvBatchPutIfAbsentRequest(const dingodb::pb::store::KvBatchPutIfAbsentRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    keys.push_back(kv.key());
  }

  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateSystemCapacity();
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchPutIfAbsent(google::protobuf::RpcController* controller,
                                          const pb::store::KvBatchPutIfAbsentRequest* request,
                                          pb::store::KvBatchPutIfAbsentResponse* response,
                                          google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvBatchPutIfAbsent request: " << request->ShortDebugString();
  if (request->kvs().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchPutIfAbsentRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvBatchPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  auto* mut_request = const_cast<dingodb::pb::store::KvBatchPutIfAbsentRequest*>(request);
  status = storage_->KvPutIfAbsent(ctx, Helper::PbRepeatedToVector(mut_request->mutable_kvs()), request->is_atomic());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchPutIfAbsent request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvBatchDeleteRequest(const dingodb::pb::store::KvBatchDeleteRequest* request) {
  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }

    keys.push_back(key);
  }

  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchDelete(google::protobuf::RpcController* controller,
                                     const pb::store::KvBatchDeleteRequest* request,
                                     pb::store::KvBatchDeleteResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvBatchDelete request: " << request->ShortDebugString();
  if (request->keys().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchDeleteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvBatchDelete request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchDeleteRequest*>(request);
  status = storage_->KvDelete(ctx, Helper::PbRepeatedToVector(mut_request->mutable_keys()));
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchDelete request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvDeleteRangeRequest(store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region at server %lu", Server::GetInstance()->Id());
  }

  auto status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->RawRange(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvDeleteRange(google::protobuf::RpcController* controller,
                                     const pb::store::KvDeleteRangeRequest* request,
                                     pb::store::KvDeleteRangeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << "KvDeleteRange request: " << request->ShortDebugString();

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(request->region_id());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvDeleteRangeRequest(region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<pb::error::Errno>(status.error_code()));
      err->set_errmsg(status.error_str());
      DINGO_LOG(ERROR) << fmt::format("KvDeleteRange request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("KvDeleteRange range invalid request: {} uniform_range: {}",
                                        request->ShortDebugString(), uniform_range.ShortDebugString());
    }
    return;
  }

  auto correction_range = Helper::IntersectRange(region->RawRange(), uniform_range);

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvDeleteRangeRequest*>(request);
  status = storage_->KvDeleteRange(ctx, correction_range);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), region->Id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard const done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvDeleteRange request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvCompareAndSetRequest(const dingodb::pb::store::KvCompareAndSetRequest* request) {
  if (request->kv().key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->kv().key()};
  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvCompareAndSet(google::protobuf::RpcController* controller,
                                       const pb::store::KvCompareAndSetRequest* request,
                                       pb::store::KvCompareAndSetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << fmt::format("KvCompareAndSet request: {}", request->ShortDebugString());

  butil::Status status = ValidateKvCompareAndSetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), request, response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  // auto* mut_request = const_cast<dingodb::pb::store::KvCompareAndSetRequest*>(request);

  status = storage_->KvCompareAndSet(ctx, {request->kv()}, {request->expect_value()}, true);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvBatchCompareAndSetRequest(const dingodb::pb::store::KvBatchCompareAndSetRequest* request) {
  for (const auto& kv : request->kvs()) {
    if (kv.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }
  }

  if (request->expect_values().size() != request->kvs().size()) {
    return butil::Status(pb::error::EKEY_EMPTY, "expect_values size !=  kvs size");
  }

  std::vector<std::string_view> keys;
  for (const auto& kv : request->kvs()) {
    keys.push_back(kv.key());
  }
  auto status = ServiceHelper::ValidateRegion(request->region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvBatchCompareAndSet(google::protobuf::RpcController* controller,
                                            const pb::store::KvBatchCompareAndSetRequest* request,
                                            pb::store::KvBatchCompareAndSetResponse* response,
                                            google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  DINGO_LOG(DEBUG) << fmt::format("KvBatchCompareAndSet request: {}", request->ShortDebugString());

  if (request->kvs().empty()) {
    return;
  }

  butil::Status status = ValidateKvBatchCompareAndSetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvBatchCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done_guard.release(), response);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);
  auto* mut_request = const_cast<dingodb::pb::store::KvBatchCompareAndSetRequest*>(request);

  status = storage_->KvCompareAndSet(ctx, Helper::PbRepeatedToVector(mut_request->kvs()),
                                     Helper::PbRepeatedToVector(mut_request->expect_values()), request->is_atomic());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    brpc::ClosureGuard done_guard(done);
    DINGO_LOG(ERROR) << fmt::format("KvBatchCompareAndSet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
  }
}

butil::Status ValidateKvScanBeginRequest(store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region at server %lu", Server::GetInstance()->Id());
  }

  auto status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->RawRange(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanBegin(google::protobuf::RpcController* controller,
                                   const ::dingodb::pb::store::KvScanBeginRequest* request,
                                   ::dingodb::pb::store::KvScanBeginResponse* response,
                                   ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto region = Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(request->region_id());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateKvScanBeginRequest(region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());

      DINGO_LOG(ERROR) << fmt::format("KvScanBegin request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("KvScanBegin range invalid request: {} uniform_range: {}",
                                        request->ShortDebugString(), uniform_range.ShortDebugString());
    }
    return;
  }
  auto correction_range = Helper::IntersectRange(region->RawRange(), uniform_range);

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  std::string scan_id;                    // NOLINT

  status = storage_->KvScanBegin(ctx, Constant::kStoreDataCF, request->region_id(), correction_range,
                                 request->max_fetch_cnt(), request->key_only(), request->disable_auto_release(),
                                 request->disable_coprocessor(), request->coprocessor(), &scan_id, &kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvScanBegin request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  *response->mutable_scan_id() = scan_id;

  DINGO_LOG(DEBUG) << fmt::format("KvScanBegin request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvScanContinueRequest(const dingodb::pb::store::KvScanContinueRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region %lu at server %lu", request->region_id(),
                         Server::GetInstance()->Id());
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  if (0 == request->max_fetch_cnt()) {
    return butil::Status(pb::error::EKEY_EMPTY, "max_fetch_cnt is 0");
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanContinue(google::protobuf::RpcController* controller,
                                      const ::dingodb::pb::store::KvScanContinueRequest* request,
                                      ::dingodb::pb::store::KvScanContinueResponse* response,
                                      ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanContinueRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvScanContinue request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanContinue(ctx, request->scan_id(), request->max_fetch_cnt(), &kvs);

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvScanContinue request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  DINGO_LOG(DEBUG) << fmt::format("KvScanContinue request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateKvScanReleaseRequest(const dingodb::pb::store::KvScanReleaseRequest* request) {
  // Check is exist region.
  if (!Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->IsExistRegion(request->region_id())) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region %lu at server %lu", request->region_id(),
                         Server::GetInstance()->Id());
  }

  if (request->scan_id().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "scan_id is empty");
  }

  return butil::Status();
}

void StoreServiceImpl::KvScanRelease(google::protobuf::RpcController* controller,
                                     const ::dingodb::pb::store::KvScanReleaseRequest* request,
                                     ::dingodb::pb::store::KvScanReleaseResponse* response,
                                     ::google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateKvScanReleaseRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("KvScanRelease request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> const ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::common::KeyValue> kvs;  // NOLINT
  status = storage_->KvScanRelease(ctx, request->scan_id());

  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->region_id(), status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("KvScanRelease request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("KvScanRelease request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

void StoreServiceImpl::SetStorage(std::shared_ptr<Storage> storage) { storage_ = storage; }

// txn

butil::Status ValidateTxnGetRequest(const dingodb::pb::store::TxnGetRequest* request) {
  if (request->key().empty()) {
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  std::vector<std::string_view> keys = {request->key()};
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnGet(google::protobuf::RpcController* controller, const pb::store::TxnGetRequest* request,
                              pb::store::TxnGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  std::vector<std::string> keys;
  auto* mut_request = const_cast<dingodb::pb::store::TxnGetRequest*>(request);
  keys.emplace_back(std::move(*mut_request->release_key()));
  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnBatchGet(ctx, keys, txn_result_info, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    response->set_value(kvs[0].value());
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnScanRequest(store::RegionPtr region, const pb::common::Range& req_range) {
  if (region == nullptr) {
    return butil::Status(pb::error::EREGION_NOT_FOUND, "Not found region");
  }

  auto status = ServiceHelper::ValidateRange(req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRangeInRange(region->RawRange(), req_range);
  if (!status.ok()) {
    return status;
  }

  status = ServiceHelper::ValidateRegionState(region);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnScan(google::protobuf::RpcController* controller, const pb::store::TxnScanRequest* request,
                               pb::store::TxnScanResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  auto region =
      Server::GetInstance()->GetStoreMetaManager()->GetStoreRegionMeta()->GetRegion(request->context().region_id());
  auto uniform_range = Helper::TransformRangeWithOptions(request->range());
  butil::Status status = ValidateTxnScanRequest(region, uniform_range);
  if (!status.ok()) {
    if (pb::error::ERANGE_INVALID != static_cast<pb::error::Errno>(status.error_code())) {
      auto* err = response->mutable_error();
      err->set_errcode(static_cast<Errno>(status.error_code()));
      err->set_errmsg(status.error_str());

      DINGO_LOG(ERROR) << fmt::format("TxnKvScan request: {} response: {}", request->ShortDebugString(),
                                      response->ShortDebugString());
    } else {
      DINGO_LOG(WARNING) << fmt::format("TxnKvScan range invalid request: {} uniform_range: {}",
                                        request->ShortDebugString(), uniform_range.ShortDebugString());
    }
    return;
  }
  auto correction_range = Helper::IntersectRange(region->RawRange(), uniform_range);

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::common::KeyValue> kvs;
  bool has_more;
  std::string end_key;

  status =
      storage_->TxnScan(ctx, request->context().region_id(), correction_range, request->limit(), request->start_ts(),
                        request->key_only(), request->is_reverse(), request->disable_coprocessor(),
                        request->coprocessor(), txn_result_info, kvs, has_more, end_key);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnKvScan request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    Helper::VectorToPbRepeated(kvs, response->mutable_kvs());
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    response->mutable_txn_result()->CopyFrom(txn_result_info);
  }
  response->set_end_key(end_key);
  response->set_has_more(has_more);

  DINGO_LOG(DEBUG) << fmt::format("TxnKvScan request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnPrewriteRequest(const dingodb::pb::store::TxnPrewriteRequest* request) {
  if (request->mutations_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations is empty");
  }

  if (request->mutations_size() > FLAGS_max_prewrite_count) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "mutations size is too large, max=1024");
  }

  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ttl is 0");
  }

  if (request->txn_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "txn_size is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& mutation : request->mutations()) {
    if (mutation.key().empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(mutation.key());
  }
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnPrewrite(google::protobuf::RpcController* controller,
                                   const pb::store::TxnPrewriteRequest* request,
                                   pb::store::TxnPrewriteResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnPrewriteRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnPrewrite request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<pb::store::Mutation> mutations;
  for (const auto& mutation : request->mutations()) {
    mutations.emplace_back(mutation);
  }

  pb::store::TxnResultInfo txn_result_info;
  std::vector<std::string> already_exists;
  uint64_t one_pc_commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnPrewrite(ctx, mutations, request->primary_lock(), request->start_ts(), request->lock_ttl(),
                                 request->txn_size(), request->try_one_pc(), request->max_commit_ts(), txn_result_info,
                                 already_exists, one_pc_commit_ts);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnPrewrite request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_one_pc_commit_ts(one_pc_commit_ts);
  for (const auto& key : already_exists) {
    response->add_keys_already_exist()->set_key(key);
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnPrewrite request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnCommitRequest(const dingodb::pb::store::TxnCommitRequest* request) {
  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "keys is empty");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnCommit(google::protobuf::RpcController* controller,
                                 const pb::store::TxnCommitRequest* request, pb::store::TxnCommitResponse* response,
                                 google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnCommitRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnCommit request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;
  uint64_t commit_ts = 0;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnCommit(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info, commit_ts);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnCommit request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_commit_ts(commit_ts);

  DINGO_LOG(DEBUG) << fmt::format("TxnCommit request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnCheckTxnStatusRequest(const dingodb::pb::store::TxnCheckTxnStatusRequest* request) {
  if (request->primary_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_key is empty");
  }

  if (request->lock_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "lock_ts is 0");
  }

  if (request->caller_start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "caller_start_ts is 0");
  }

  if (request->current_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "current_ts is 0");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_key());
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnCheckTxnStatus(google::protobuf::RpcController* controller,
                                         const pb::store::TxnCheckTxnStatusRequest* request,
                                         pb::store::TxnCheckTxnStatusResponse* response,
                                         google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnCheckTxnStatusRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnCheckTxnStatus request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  pb::store::TxnResultInfo txn_result_info;
  uint64_t lock_ttl = 0;
  uint64_t commit_ts = 0;
  pb::store::Action action;
  pb::store::LockInfo lock_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnCheckTxnStatus(ctx, request->primary_key(), request->lock_ts(), request->caller_start_ts(),
                                       request->current_ts(), txn_result_info, lock_ttl, commit_ts, action, lock_info);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnCheckTxnStatus request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_lock_ttl(lock_ttl);
  response->set_commit_ts(commit_ts);
  response->set_action(action);
  response->mutable_lock_info()->CopyFrom(lock_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnCheckTxnStatus request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnResolveLockRequest(const dingodb::pb::store::TxnResolveLockRequest* request) {
  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->commit_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "commit_ts is 0");
  }

  if (request->keys_size() > 0) {
    for (const auto& key : request->keys()) {
      if (key.empty()) {
        return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
      }
      std::vector<std::string_view> keys;
      keys.push_back(key);
      auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
      if (!status.ok()) {
        return status;
      }
    }
  }

  return butil::Status();
}

void StoreServiceImpl::TxnResolveLock(google::protobuf::RpcController* controller,
                                      const pb::store::TxnResolveLockRequest* request,
                                      pb::store::TxnResolveLockResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnResolveLockRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnResolveLock request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnResolveLock(ctx, request->start_ts(), request->commit_ts(), keys, txn_result_info);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnResolveLock request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnResolveLock request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnBatchGetRequest(const dingodb::pb::store::TxnBatchGetRequest* request) {
  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnBatchGet(google::protobuf::RpcController* controller,
                                   const pb::store::TxnBatchGetRequest* request,
                                   pb::store::TxnBatchGetResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnBatchGetRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnBatchGet(ctx, keys, txn_result_info, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnBatchGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  if (!kvs.empty()) {
    for (const auto& kv : kvs) {
      response->add_kvs()->CopyFrom(kv);
    }
  }
  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnBatchGet request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnBatchRollbackRequest(const dingodb::pb::store::TxnBatchRollbackRequest* request) {
  if (request->keys_size() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "Keys is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  std::vector<std::string_view> keys;
  for (const auto& key : request->keys()) {
    if (key.empty()) {
      return butil::Status(pb::error::EKEY_EMPTY, "key is empty");
    }
    keys.push_back(key);
  }
  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnBatchRollback(google::protobuf::RpcController* controller,
                                        const pb::store::TxnBatchRollbackRequest* request,
                                        pb::store::TxnBatchRollbackResponse* response,
                                        google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnBatchRollbackRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);
  std::vector<std::string> keys;
  for (const auto& key : request->keys()) {
    keys.emplace_back(key);
  }

  pb::store::TxnResultInfo txn_result_info;

  std::vector<pb::common::KeyValue> kvs;
  status = storage_->TxnBatchRollback(ctx, request->start_ts(), keys, txn_result_info, kvs);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnBatchRollback request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnBatchRollback request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnScanLockRequest(const dingodb::pb::store::TxnScanLockRequest* request) {
  if (request->max_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "max_ts is 0");
  }

  if (request->limit() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "limit is 0");
  }

  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->start_key());

  std::string str(1, '\x01');
  std::string end_key = Helper::StringSubtractRightAlign(request->end_key(), str);

  keys.push_back(end_key);

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnScanLock(google::protobuf::RpcController* controller,
                                   const pb::store::TxnScanLockRequest* request,
                                   pb::store::TxnScanLockResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnScanLockRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::LockInfo> locks;

  status = storage_->TxnScanLock(ctx, request->max_ts(), request->start_key(), request->limit(), request->end_key(),
                                 txn_result_info, locks);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnScanLock request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  for (const auto& lock : locks) {
    response->add_locks()->CopyFrom(lock);
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnScanLock request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnHeartBeatRequest(const dingodb::pb::store::TxnHeartBeatRequest* request) {
  if (request->primary_lock().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "primary_lock is empty");
  }

  if (request->start_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_ts is 0");
  }

  if (request->advise_lock_ttl() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "advise_lock_ttl is 0");
  }

  std::vector<std::string_view> keys;
  keys.push_back(request->primary_lock());

  auto status = ServiceHelper::ValidateRegion(request->context().region_id(), keys);
  if (!status.ok()) {
    return status;
  }

  return butil::Status();
}

void StoreServiceImpl::TxnHeartBeat(google::protobuf::RpcController* controller,
                                    const pb::store::TxnHeartBeatRequest* request,
                                    pb::store::TxnHeartBeatResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnHeartBeatRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  pb::store::TxnResultInfo txn_result_info;
  uint64_t lock_ttl = 0;

  status = storage_->TxnHeartBeat(ctx, request->primary_lock(), request->start_ts(), request->advise_lock_ttl(),
                                  txn_result_info, lock_ttl);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnHeartBeat request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);
  response->set_lock_ttl(lock_ttl);

  DINGO_LOG(DEBUG) << fmt::format("TxnHeartBeat request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnGcRequest(const dingodb::pb::store::TxnGcRequest* request) {
  if (request->safe_point_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "safe_point_ts is 0");
  }

  return butil::Status();
}

void StoreServiceImpl::TxnGc(google::protobuf::RpcController* controller, const pb::store::TxnGcRequest* request,
                             pb::store::TxnGcResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnGcRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  pb::store::TxnResultInfo txn_result_info;
  uint64_t lock_ttl = 0;

  status = storage_->TxnGc(ctx, request->safe_point_ts(), txn_result_info);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnGc request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  response->mutable_txn_result()->CopyFrom(txn_result_info);

  DINGO_LOG(DEBUG) << fmt::format("TxnGc request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnDeleteRangeRequest(const dingodb::pb::store::TxnDeleteRangeRequest* request) {
  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  return butil::Status();
}

void StoreServiceImpl::TxnDeleteRange(google::protobuf::RpcController* controller,
                                      const pb::store::TxnDeleteRangeRequest* request,
                                      pb::store::TxnDeleteRangeResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnDeleteRangeRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  status = storage_->TxnDeleteRange(ctx, request->start_key(), request->end_key());
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnDeleteRange request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnDeleteRange request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

butil::Status ValidateTxnDumpRequest(const dingodb::pb::store::TxnDumpRequest* request) {
  if (request->start_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "start_key is empty");
  }

  if (request->end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_key is empty");
  }

  if (request->end_ts() == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "end_ts is 0");
  }

  return butil::Status();
}

void StoreServiceImpl::TxnDump(google::protobuf::RpcController* controller, const pb::store::TxnDumpRequest* request,
                               pb::store::TxnDumpResponse* response, google::protobuf::Closure* done) {
  brpc::Controller* cntl = (brpc::Controller*)controller;
  brpc::ClosureGuard done_guard(done);

  butil::Status status = ValidateTxnDumpRequest(request);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    DINGO_LOG(ERROR) << fmt::format("TxnGet request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  std::shared_ptr<Context> ctx = std::make_shared<Context>(cntl, done);
  ctx->SetRegionId(request->context().region_id()).SetCfName(Constant::kStoreDataCF);

  pb::store::TxnResultInfo txn_result_info;
  std::vector<pb::store::TxnWriteKey> txn_write_keys;
  std::vector<pb::store::TxnWriteValue> txn_write_values;
  std::vector<pb::store::TxnLockKey> txn_lock_keys;
  std::vector<pb::store::TxnLockValue> txn_lock_values;
  std::vector<pb::store::TxnDataKey> txn_data_keys;
  std::vector<pb::store::TxnDataValue> txn_data_values;

  status = storage_->TxnDump(ctx, request->start_key(), request->end_key(), request->start_ts(), request->end_ts(),
                             txn_result_info, txn_write_keys, txn_write_values, txn_lock_keys, txn_lock_values,
                             txn_data_keys, txn_data_values);
  if (!status.ok()) {
    auto* err = response->mutable_error();
    err->set_errcode(static_cast<Errno>(status.error_code()));
    err->set_errmsg(status.error_str());
    if (status.error_code() == pb::error::ERAFT_NOTLEADER) {
      err->set_errmsg(fmt::format("Not leader({}) on region {}, please redirect leader({}).",
                                  Server::GetInstance()->ServerAddr(), request->context().region_id(),
                                  status.error_str()));
      ServiceHelper::RedirectLeader(status.error_str(), response);
    }
    DINGO_LOG(ERROR) << fmt::format("TxnDump request: {} response: {}", request->ShortDebugString(),
                                    response->ShortDebugString());
    return;
  }

  DINGO_LOG(DEBUG) << fmt::format("TxnDump request: {} response: {}", request->ShortDebugString(),
                                  response->ShortDebugString());
}

}  // namespace dingodb
