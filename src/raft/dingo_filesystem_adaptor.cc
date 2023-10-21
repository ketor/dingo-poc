// Copyright(c) 2023 dingodb.com, Inc.All Rights Reserved
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

#include "raft/dingo_filesystem_adaptor.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/synchronization.h"
#include "config/config_manager.h"
#include "engine/iterator.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_int64(snapshot_timeout_min, 10, "snapshot_timeout_min : 10min");

bool inline IsSnapshotMetaFile(const std::string& path) {
  butil::StringPiece sp(path);
  return sp.ends_with(kSnapshotMetaFile);
}

bool inline IsSnapshotDataFile(const std::string& path) {
  butil::StringPiece sp(path);
  return sp.ends_with(kSnapshotDataFile);
}

std::string GetSnapshotPath(const std::string& path) {
  // the path is like /data/snapshot_00001/default.sst
  // the snapshot_path is like /data/snapshot_00001
  std::filesystem::path p(path);
  return p.parent_path().string();
}

bool PosixDirReader::is_valid() const { return dir_reader_.IsValid(); }

bool PosixDirReader::next() {
  bool rc = dir_reader_.Next();
  while (rc && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
    rc = dir_reader_.Next();
  }
  return rc;
}

const char* PosixDirReader::name() const { return dir_reader_.name(); }

// DingoDataReaderAdaptor
DingoDataReaderAdaptor::DingoDataReaderAdaptor(int64_t region_id, const std::string& path, DingoFileSystemAdaptor* rs,
                                               IteratorContextPtr context)
    : region_id_(region_id), path_(path), rs_(rs), context_(context) {
  region_ptr_ = Server::GetInstance().GetRegion(region_id_);
}

DingoDataReaderAdaptor::~DingoDataReaderAdaptor() { close(); }

bool DingoDataReaderAdaptor::RegionShutdown() {
  return region_ptr_ == nullptr || region_ptr_->State() == pb::common::StoreRegionState::DELETING ||
         region_ptr_->State() == pb::common::StoreRegionState::DELETED ||
         region_ptr_->State() == pb::common::StoreRegionState::STANDBY;
}

void DingoDataReaderAdaptor::ContextReset() {
  IteratorContextPtr iter_context = nullptr;
  iter_context.reset(new IteratorContext);
  if (iter_context == nullptr) {
    return;
  }
  iter_context->region_id = context_->region_id;
  iter_context->cf_id = context_->cf_id;
  iter_context->reading = context_->reading;
  iter_context->lower_bound = context_->lower_bound;
  iter_context->upper_bound = context_->upper_bound;
  iter_context->applied_index = context_->applied_index;
  iter_context->need_copy_data = context_->need_copy_data;
  iter_context->snapshot_context = context_->snapshot_context;
  IteratorOptions iter_options;
  iter_options.lower_bound = iter_context->lower_bound;
  iter_options.upper_bound = iter_context->upper_bound;
  auto new_iter = context_->reader->NewIterator(context_->snapshot_context->snapshot, IteratorOptions());
  iter_context->iter = new_iter;
  iter_context->iter->Seek(iter_context->lower_bound);
  iter_context->snapshot_context->data_iterators[context_->cf_id] = iter_context;
  context_ = iter_context;
}

ssize_t DingoDataReaderAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
  if (closed_) {
    DINGO_LOG(ERROR) << "rocksdb reader has been closed, region_id: " << region_id_ << ", offset: " << offset;
    return -1;
  }
  if (offset < 0) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " read error. offset: " << offset;
    return -1;
  }

  if (RegionShutdown()) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " shutdown, last_off: " << last_offset_ << ", off: " << offset
                     << ", ctx->off: " << context_->offset << ", size: " << size;
    return -1;
  }

  TimeCost time_cost;
  if (!context_->need_copy_data) {
    context_->done = true;
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " need not copy data, time_cost: " << time_cost.GetTime();
    return 0;
  }
  if (offset > context_->offset) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_
                     << " retry last_offset, offset biger fail, time_cost: " << time_cost.GetTime()
                     << ", last_off: " << last_offset_ << ", off: " << offset << ", ctx->off: " << context_->offset
                     << ", size: " << size;
    return -1;
  }
  if (offset < context_->offset) {
    // cache the last package, when retry first time, can recover
    if (last_offset_ == offset) {
      *portal = last_package_;
      DINGO_LOG(ERROR) << "region_id: " << region_id_ << " retry last_offset, time_cost: " << time_cost.GetTime()
                       << ", last_off: " << last_offset_ << ", off: " << offset << ", ctx->off: " << context_->offset
                       << ", size: " << size << ", ret_size: " << last_package_.size();
      return last_package_.size();
    }

    // reset context_
    if (offset == 0 && context_->offset_update_time.GetTime() > FLAGS_snapshot_timeout_min * 60 * 1000 * 1000ULL) {
      last_offset_ = 0;
      num_lines_ = 0;
      ContextReset();
      DINGO_LOG(ERROR) << "region_id: " << region_id_ << " context_reset, time_cost: " << time_cost.GetTime()
                       << ", last_off: " << last_offset_ << ", off: " << offset << ", ctx->off: " << context_->offset
                       << ", size: " << size;
    } else {
      DINGO_LOG(ERROR) << "region_id: " << region_id_ << " retry last_offset fail, time_cost: " << time_cost.GetTime()
                       << ", last_off: " << last_offset_ << ", off: " << offset << ", ctx->off: " << context_->offset
                       << ", size: " << size;
      return -1;
    }
  }

  size_t count = 0;
  int64_t key_num = 0;
  // 大region addpeer中重置time_cost，防止version=0超时删除
  // region_ptr_->reset_timecost();

  while (count < size) {
    if (!context_->iter->Valid() || !(context_->iter->Key() >= context_->lower_bound)) {
      context_->done = true;
      // portal->append((void*)iter_context->offset, sizeof(size_t));
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot read over, total size: " << context_->offset;
      // auto region = Store::get_instance()->get_region(region_id_);
      auto region = region_ptr_;
      if (region == nullptr) {
        DINGO_LOG(ERROR) << "region_id: " << region_id_ << " is null region";
        return -1;
      }
      // region->set_snapshot_data_size(context_->offset);
      break;
    }
    int64_t read_size = 0;
    key_num++;
    read_size += SerializeToIobuf(portal, context_->iter->Key());
    read_size += SerializeToIobuf(portal, context_->iter->Value());
    count += read_size;
    ++num_lines_;
    context_->offset += read_size;
    context_->offset_update_time.Reset();
    context_->iter->Next();
  }
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " read done. count: " << count << ", key_num: " << key_num
                     << ", time_cost: " << time_cost.GetTime() << ", off: " << offset << ", size: " << size
                     << ", last_off: " << last_offset_ << ", last_count: " << last_package_.size();
  last_offset_ = offset;
  last_package_ = *portal;
  return count;
}

bool DingoDataReaderAdaptor::close() {
  if (closed_) {
    DINGO_LOG(WARNING) << "file has been closed, region_id: " << region_id_ << ", num_lines: " << num_lines_
                       << ", path: " << path_;
    return true;
  }
  rs_->Close(path_);
  closed_ = true;
  return true;
}

ssize_t DingoDataReaderAdaptor::size() {
  if (context_->done) {
    return context_->offset;
  }
  return std::numeric_limits<ssize_t>::max();
}

ssize_t DingoDataReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
  (void)data;
  (void)offset;
  DINGO_LOG(ERROR) << "DingoReaderAdaptor::write not implemented";
  return -1;
}

bool DingoDataReaderAdaptor::sync() { return true; }

// DingoMetaReaderAdaptor
DingoMetaReaderAdaptor::DingoMetaReaderAdaptor(int64_t region_id, const std::string& path, int64_t applied_term,
                                               int64_t applied_index, pb::common::RegionEpoch& region_epoch,
                                               pb::common::Range& range)
    : region_id_(region_id),
      path_(path),
      applied_term_(applied_term),
      applied_index_(applied_index),
      region_epoch_(region_epoch),
      range_(range) {
  region_ptr_ = Server::GetInstance().GetRegion(region_id_);

  pb::store_internal::RaftSnapshotRegionMeta meta;
  *(meta.mutable_epoch()) = region_epoch;
  *(meta.mutable_range()) = range;
  meta.set_term(applied_term);
  meta.set_log_index(applied_index);

  this->meta_data_ = meta.SerializeAsString();
}

DingoMetaReaderAdaptor::~DingoMetaReaderAdaptor() { close(); }

bool DingoMetaReaderAdaptor::RegionShutdown() {
  return region_ptr_ == nullptr || region_ptr_->State() == pb::common::StoreRegionState::DELETING ||
         region_ptr_->State() == pb::common::StoreRegionState::DELETED ||
         region_ptr_->State() == pb::common::StoreRegionState::STANDBY;
}

ssize_t DingoMetaReaderAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
  if (closed_) {
    DINGO_LOG(ERROR) << "rocksdb reader has been closed, region_id: " << region_id_ << ", offset: " << offset;
    return -1;
  }

  if (offset < 0) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " read error. offset: " << offset;
    return -1;
  }

  if (RegionShutdown()) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " shutdown, off: " << offset << ", size: " << size;
    return -1;
  }

  TimeCost time_cost;

  if (offset > meta_data_.size() - 1) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_
                     << " retry last_offset, offset biger fail, time_cost: " << time_cost.GetTime()
                     << ", off: " << offset << ", size: " << size;
    return -1;
  }

  size_t count = SerializeToIobuf(portal, meta_data_.substr(offset));
  DINGO_LOG(INFO) << "region_id: " << region_id_ << " read meta, time_cost: " << time_cost.GetTime()
                  << ", off: " << offset << ", size: " << size << ", ret_size: " << count;

  return count;
}

bool DingoMetaReaderAdaptor::close() {
  if (closed_) {
    DINGO_LOG(WARNING) << "file has been closed, region_id: " << region_id_ << ", path: " << path_;
    return true;
  }
  closed_ = true;
  return true;
}

ssize_t DingoMetaReaderAdaptor::size() { return meta_data_.size(); }

ssize_t DingoMetaReaderAdaptor::write(const butil::IOBuf& data, off_t offset) {
  (void)data;
  (void)offset;
  DINGO_LOG(ERROR) << "DingoReaderAdaptor::write not implemented";
  return -1;
}

bool DingoMetaReaderAdaptor::sync() { return true; }

// SstWriterAdaptor
bool SstWriterAdaptor::RegionShutdown() {
  return region_ptr_ == nullptr || region_ptr_->State() == pb::common::StoreRegionState::DELETING ||
         region_ptr_->State() == pb::common::StoreRegionState::DELETED ||
         region_ptr_->State() == pb::common::StoreRegionState::STANDBY;
}

SstWriterAdaptor::SstWriterAdaptor(int64_t region_id, const std::string& path, const rocksdb::Options& option)
    : region_id_(region_id), path_(path), writer_(new SstFileWriter(option)) {}

int SstWriterAdaptor::Open() {
  region_ptr_ = Server::GetInstance().GetRegion(region_id_);
  if (region_ptr_ == nullptr) {
    DINGO_LOG(ERROR) << "open sst file path: " << path_ << " failed, region_id: " << region_id_ << " not exist";
    return -1;
  }
  std::string path = path_;
  auto s = writer_->Open(path);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << "open sst file path: " << path << " failed, err: " << s.ToString()
                     << ", region_id: " << region_id_;
    return -1;
  }
  closed_ = false;
  DINGO_LOG(WARNING) << "rocksdb sst writer open, path: " << path << ", region_id: " << region_id_;
  return 0;
}

ssize_t SstWriterAdaptor::write(const butil::IOBuf& data, off_t offset) {
  (void)offset;
  std::string path = path_;
  if (RegionShutdown()) {
    DINGO_LOG(ERROR) << "write sst file path: " << path << " failed, region shutdown, data len: " << data.size()
                     << ", region_id: " << region_id_;
    return -1;
  }
  if (closed_) {
    DINGO_LOG(ERROR) << "write sst file path: " << path << " failed, file closed: " << closed_
                     << ", data len: " << data.size() << ", region_id: " << region_id_;
    return -1;
  }
  if (data.empty()) {
    DINGO_LOG(WARNING) << "write sst file path: " << path << " failed, data len = 0, region_id: " << region_id_;
  }
  auto ret = IobufToSst(data);
  if (ret < 0) {
    DINGO_LOG(ERROR) << "write sst file path: " << path << " failed, received invalid data, data len: " << data.size()
                     << ", region_id: " << region_id_;
    return -1;
  }
  data_size_ += data.size();

  DINGO_LOG(WARNING) << "rocksdb sst write, region_id: " << region_id_ << ", path: " << path << ", offset: " << offset
                     << ", data.size: " << data.size() << ", file_size: " << writer_->FileSize()
                     << ", total_count: " << count_ << ", all_size: " << data_size_;

  return data.size();
}

bool SstWriterAdaptor::close() {
  if (closed_) {
    DINGO_LOG(WARNING) << "file has been closed, path: " << path_;
    return true;
  }
  closed_ = true;
  return FinishSst();
}

bool SstWriterAdaptor::FinishSst() {
  std::string path = path_;
  if (count_ > 0) {
    DINGO_LOG(WARNING) << "writer_ finished, path: " << path << ", region_id: " << region_id_
                       << ", file_size: " << writer_->FileSize() << ", total_count: " << count_
                       << ", all_size: " << data_size_;
    auto s = writer_->Finish();
    if (!s.ok()) {
      DINGO_LOG(ERROR) << "finish sst file path: " << path << " failed, err: " << s.ToString()
                       << ", region_id: " << region_id_;
      return false;
    }
  } else {
    bool ret = butil::DeleteFile(butil::FilePath(path), false);
    DINGO_LOG(WARNING) << "count is 0, delete path: " << path << ", region_id: " << region_id_;
    if (!ret) {
      DINGO_LOG(ERROR) << "delete sst file path: " << path << " failed, region_id: " << region_id_;
    }
  }
  return true;
}

int SstWriterAdaptor::IobufToSst(butil::IOBuf data) {
  char key_buf[4 * 1024];
  // 32KB stack should be enough for most cases
  char value_buf[32 * 1024];
  while (!data.empty()) {
    size_t key_size = 0;
    size_t nbytes = data.cutn((void*)&key_size, sizeof(size_t));
    if (nbytes < sizeof(size_t)) {
      DINGO_LOG(ERROR) << "read key size from iobuf fail, region_id: " << region_id_;
      return -1;
    }
    rocksdb::Slice key;
    std::unique_ptr<char[]> big_key_buf;
    // sst_file_writer does not support SliceParts, using fetch can try to 0 copy
    if (key_size <= sizeof(key_buf)) {
      key.data_ = static_cast<const char*>(data.fetch(key_buf, key_size));
    } else {
      big_key_buf.reset(new char[key_size]);
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << ", key_size: " << key_size << " too big";
      key.data_ = static_cast<const char*>(data.fetch(big_key_buf.get(), key_size));
    }
    key.size_ = key_size;
    if (key.data_ == nullptr) {
      DINGO_LOG(ERROR) << "read key from iobuf fail, region_id: " << region_id_ << ", key_size: " << key_size;
      return -1;
    }
    data.pop_front(key_size);

    size_t value_size = 0;
    nbytes = data.cutn((void*)&value_size, sizeof(size_t));
    if (nbytes < sizeof(size_t)) {
      DINGO_LOG(ERROR) << "read value size from iobuf fail, region_id: " << region_id_ << ", value_size: " << value_size
                       << ", key_size: " << key_size << ", key: " << key.ToString(true);
      return -1;
    }
    rocksdb::Slice value;
    std::unique_ptr<char[]> big_value_buf;
    if (value_size <= sizeof(value_buf)) {
      if (value_size > 0) {
        value.data_ = static_cast<const char*>(data.fetch(value_buf, value_size));
      }
    } else {
      big_value_buf.reset(new char[value_size]);
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << ", value_size: " << value_size << " too big";
      value.data_ = static_cast<const char*>(data.fetch(big_value_buf.get(), value_size));
    }
    value.size_ = value_size;
    if (value.data_ == nullptr) {
      DINGO_LOG(ERROR) << "read value from iobuf, region_id: " << region_id_ << ", value_size: " << value_size
                       << ", key_size: " << key_size << ", key: " << key.ToString(true);
      return -1;
    }
    data.pop_front(value_size);
    count_++;

    auto s = writer_->Put(key, value);
    if (!s.ok()) {
      DINGO_LOG(ERROR) << "write sst file failed, err: " << s.ToString() << ", region_id: " << region_id_;
      return -1;
    }
  }
  return 0;
}

SstWriterAdaptor::~SstWriterAdaptor() { close(); }

ssize_t SstWriterAdaptor::size() {
  DINGO_LOG(ERROR) << "SstWriterAdaptor::size not implemented, region_id: " << region_id_;
  return -1;
}

bool SstWriterAdaptor::sync() {
  // already sync in SstFileWriter::Finish
  return true;
}

ssize_t SstWriterAdaptor::read(butil::IOPortal* /*portal*/, off_t /*offset*/, size_t /*size*/) {
  DINGO_LOG(ERROR) << "SstWriterAdaptor::read not implemented, region_id: " << region_id_;
  return -1;
}

// PosixFileAdaptor
PosixFileAdaptor::~PosixFileAdaptor() { close(); }

int PosixFileAdaptor::Open(int oflag) {
  oflag &= (~O_CLOEXEC);
  fd_ = ::open(path_.c_str(), oflag, 0644);
  if (fd_ <= 0) {
    return -1;
  }
  return 0;
}

bool PosixFileAdaptor::close() {
  if (fd_ > 0) {
    bool res = ::close(fd_) == 0;
    fd_ = -1;
    return res;
  }
  return true;
}

ssize_t PosixFileAdaptor::write(const butil::IOBuf& data, off_t offset) {
  ssize_t ret = braft::file_pwrite(data, fd_, offset);
  return ret;
}

ssize_t PosixFileAdaptor::read(butil::IOPortal* portal, off_t offset, size_t size) {
  return braft::file_pread(portal, fd_, offset, size);
}

ssize_t PosixFileAdaptor::size() {
  off_t sz = lseek(fd_, 0, SEEK_END);
  return ssize_t(sz);
}

bool PosixFileAdaptor::sync() { return braft::raft_fsync(fd_) == 0; }

// DingoFileSystemAdaptor
DingoFileSystemAdaptor::DingoFileSystemAdaptor(int64_t region_id) : region_id_(region_id) {}

DingoFileSystemAdaptor::~DingoFileSystemAdaptor() {
  mutil_snapshot_cond_.Wait();
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " DingoFileSystemAdaptor released";
}

braft::FileAdaptor* DingoFileSystemAdaptor::open(const std::string& path, int oflag,
                                                 const ::google::protobuf::Message* file_meta, butil::File::Error* e) {
  if (!IsSnapshotDataFile(path) && !IsSnapshotMetaFile(path)) {
    PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
    int ret = adaptor->Open(oflag);
    if (ret != 0) {
      if (e) {
        *e = butil::File::OSErrorToFileError(errno);
      }
      delete adaptor;
      return nullptr;
    }
    DINGO_LOG(WARNING) << "open file: " << path << ", region_id: " << region_id_ << ", " << (oflag & O_WRONLY);
    return adaptor;
  }

  bool for_write = (O_WRONLY & oflag);
  if (for_write) {
    if (IsSnapshotMetaFile(path)) {
      PosixFileAdaptor* adaptor = new PosixFileAdaptor(path);
      int ret = adaptor->Open(oflag);
      if (ret != 0) {
        if (e) {
          *e = butil::File::OSErrorToFileError(errno);
        }
        delete adaptor;
        return nullptr;
      }
      DINGO_LOG(WARNING) << "open file: " << path << ", region_id: " << region_id_ << ", " << (oflag & O_WRONLY);
      return adaptor;
    }
    return OpenWriterAdaptor(path, oflag, file_meta, e);
  }
  return OpenReaderAdaptor(path, oflag, file_meta, e);
}

braft::FileAdaptor* DingoFileSystemAdaptor::OpenWriterAdaptor(const std::string& path,  // NOLINT
                                                              int /*oflag*/,
                                                              const ::google::protobuf::Message* file_meta,
                                                              butil::File::Error* e) {
  (void)file_meta;

  rocksdb::Options options;
  options.bottommost_compression = rocksdb::kNoCompression;
  options.bottommost_compression_opts = rocksdb::CompressionOptions();

  SstWriterAdaptor* writer = new SstWriterAdaptor(region_id_, path, options);
  int ret = writer->Open();
  if (ret != 0) {
    if (e) {
      *e = butil::File::FILE_ERROR_FAILED;
    }
    delete writer;
    return nullptr;
  }
  DINGO_LOG(WARNING) << "open for write file, path: " << path << ", region_id: " << region_id_;
  return writer;
}

braft::FileAdaptor* DingoFileSystemAdaptor::OpenReaderAdaptor(const std::string& path, int /*oflag*/,
                                                              const ::google::protobuf::Message* file_meta,
                                                              butil::File::Error* e) {
  TimeCost time_cost;
  (void)file_meta;
  std::string lower_bound;
  std::string upper_bound;
  size_t len = path.size();

  auto region = Server::GetInstance().GetRegion(region_id_);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " is null region";
    return nullptr;
  }

  const std::string snapshot_path = GetSnapshotPath(path);
  // raft_copy_remote_file_timeout_ms is set to 300s
  // use another mutex here to avoid blocking the main thread and promise serialize
  BAIDU_SCOPED_LOCK(open_reader_adaptor_mutex_);
  auto snapshot_context = GetSnapshot(snapshot_path);
  if (snapshot_context == nullptr) {
    DINGO_LOG(ERROR) << "snapshot no found, path: " << snapshot_path << ", region_id: " << region_id_;
    if (e != nullptr) {
      *e = butil::File::FILE_ERROR_NOT_FOUND;
    }
    return nullptr;
  }

  if (IsSnapshotMetaFile(path)) {
    auto* reader =
        new DingoMetaReaderAdaptor(region_id_, path, snapshot_context->applied_term, snapshot_context->applied_index,
                                   snapshot_context->region_epoch, snapshot_context->range);
    return reader;
  }

  IteratorContextPtr iter_context = nullptr;
  if (IsSnapshotDataFile(path)) {
    // iter_context = sc->data_context;
    // // first open snapshot file
    // if (iter_context == nullptr) {
    //   iter_context.reset(new IteratorContext);
    //   iter_context->prefix = prefix;
    //   iter_context->is_meta_sst = false;
    //   iter_context->upper_bound = upper_bound;
    //   iter_context->upper_bound_slice = iter_context->upper_bound;
    //   iter_context->sc = sc.get();
    //   rocksdb::ReadOptions read_options;
    //   read_options.snapshot = sc->snapshot;
    //   read_options.total_order_seek = true;
    //   read_options.fill_cache = false;
    //   read_options.iterate_upper_bound = &iter_context->upper_bound_slice;
    //   // rocksdb::ColumnFamilyHandle* column_family = RocksWrapper::get_instance()->get_data_handle();
    //   // iter_context->iter.reset(RocksWrapper::get_instance()->new_iterator(read_options, column_family));
    //   iter_context->iter->Seek(prefix);
    //   braft::NodeStatus status;

    //   auto region = Store::get_instance()->get_region(region_id_);
    //   region->get_node_status(&status);
    //   int64_t peer_next_index = 0;
    //   // 通过peer状态和data_index判断是否需要复制数据
    //   // addpeer在unstable里，peer_next_index=0就会走复制流程
    //   for (auto iter : status.stable_followers) {
    //     auto& peer = iter.second;
    //     DINGO_LOG(WARNING) << "region_id: " << region_id_ << " " << iter.first.to_string() << " "
    //                        << peer.installing_snapshot << " " << peer.next_index;
    //     if (peer.installing_snapshot) {
    //       peer_next_index = peer.next_index;
    //       break;
    //     }
    //   }
    //   if (sc->data_index < peer_next_index) {
    //     iter_context->need_copy_data = false;
    //   }
    //   sc->data_context = iter_context;
    //   DINOG_LOG(WARNING) << "region_id: " << region_id_ << " open reader, data_index:" << sc->data_index
    //                      << ",peer_next_index:" << peer_next_index << ", path: " << path
    //                      << ", time_cost: " << time_cost.get_time();
    // }
  }
  int64_t applied_index = 0;
  int64_t data_index = 0;
  int64_t snapshot_index = 0;

  if (iter_context->reading) {
    DINGO_LOG(WARNING) << "snapshot reader is busy, path: " << path << ", region_id: " << region_id_;
    if (e != nullptr) {
      *e = butil::File::FILE_ERROR_IN_USE;
    }
    return nullptr;
  }
  iter_context->reading = true;
  auto* reader = new DingoDataReaderAdaptor(region_id_, path, this, iter_context);
  reader->Open();
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open reader: path: " << path
                     << " snapshot_index: " << snapshot_index << " applied_index: " << applied_index
                     << " data_index: " << data_index << " time_cost: " << time_cost.GetTime();
  return reader;
}

bool DingoFileSystemAdaptor::delete_file(const std::string& path, bool recursive) {
  butil::FilePath file_path(path);
  return butil::DeleteFile(file_path, recursive);
}

bool DingoFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
  return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool DingoFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
  return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool DingoFileSystemAdaptor::create_directory(const std::string& path, butil::File::Error* error,
                                              bool create_parent_directories) {
  butil::FilePath dir(path);
  return butil::CreateDirectoryAndGetError(dir, error, create_parent_directories);
}

bool DingoFileSystemAdaptor::path_exists(const std::string& path) {
  butil::FilePath file_path(path);
  return butil::PathExists(file_path);
}

bool DingoFileSystemAdaptor::directory_exists(const std::string& path) {
  butil::FilePath file_path(path);
  return butil::DirectoryExists(file_path);
}

braft::DirReader* DingoFileSystemAdaptor::directory_reader(const std::string& path) { return new PosixDirReader(path); }

bool DingoFileSystemAdaptor::open_snapshot(const std::string& path) {
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open snapshot path: " << path;
  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshots_.find(path);
  if (iter != snapshots_.end()) {
    // raft InstallSnapshot timeout -1
    // If long time no read, means the follower machine is hang
    // TODO: if long time no read, close snapshot
    if (iter->second.cost.GetTime() > 3600 * 1000 * 1000LL && iter->second.count == 1) {
      bool need_erase = true;
      for (const auto& data_iter : iter->second.ptr->data_iterators) {
        if (data_iter->offset > 0) {
          need_erase = false;
          break;
        }
      }

      if (need_erase) {
        snapshots_.erase(iter);
        DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot path: " << path
                           << " is hang over 1 hour, erase";
      } else {
        DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot path: " << path
                           << " is hang over 1 hour, but data_iterators is not empty, not erase";
      }
    } else {
      // learner pull snapshot will keep raft_snapshot_reader_expire_time_s
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << " snapshot path: " << path << " is busy";
      snapshots_[path].count++;
      return false;
    }
  }

  mutil_snapshot_cond_.Increase();
  // create new raw engine snapshot
  auto region = Server::GetInstance().GetRegion(region_id_);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << "region_id: " << region_id_ << " is null region";
    return false;
  }

  region->LockRegionMeta();
  int64_t applied_term = region->GetAppliedTerm();
  int64_t applied_index = region->GetAppliedIndex();
  auto region_epoch = region->Epoch(true);
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open snapshot path: " << path;
  snapshots_[path].ptr.reset(new SnapshotContext());
  snapshots_[path].ptr->applied_term = applied_term;
  snapshots_[path].ptr->applied_index = applied_index;
  snapshots_[path].ptr->region_epoch = region_epoch;
  snapshots_[path].ptr->range = region->Range();
  region->UnlockRegionMeta();

  snapshots_[path].count++;
  snapshots_[path].cost.Reset();
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " open snapshot path: " << path
                     << ", applied_index: " << applied_index;
  return true;
}

void DingoFileSystemAdaptor::close_snapshot(const std::string& path) {
  DINGO_LOG(WARNING) << "region_id: " << region_id_ << " close snapshot path: " << path;
  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshots_.find(path);
  if (iter != snapshots_.end()) {
    snapshots_[path].count--;
    if (snapshots_[path].count == 0) {
      snapshots_.erase(iter);
      mutil_snapshot_cond_.DecreaseBroadcast();
      DINGO_LOG(WARNING) << "region_id: " << region_id_ << " close snapshot path: " << path << " relase";
    }
  }
}

SnapshotContextPtr DingoFileSystemAdaptor::GetSnapshot(const std::string& path) {
  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshots_.find(path);
  if (iter != snapshots_.end()) {
    return iter->second.ptr;
  }
  return nullptr;
}

void DingoFileSystemAdaptor::Close(const std::string& path) {
  const std::string snapshot_path = GetSnapshotPath(path);

  BAIDU_SCOPED_LOCK(snapshot_mutex_);
  auto iter = snapshots_.find(snapshot_path);
  if (iter == snapshots_.end()) {
    DINGO_LOG(ERROR) << "no snapshot found when close reader, path: " << path << ", region_id: " << region_id_;
    return;
  }

  auto& snapshot_ctx = iter->second;
  for (auto& data_iterator : snapshot_ctx.ptr->data_iterators) {
    data_iterator = nullptr;
  }
  DINGO_LOG(WARNING) << "close snapshot data file, path: " << path << ", region_id: " << region_id_;
}

}  // namespace dingodb