// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <Storages/DeltaMerge/ExternalDTFileInfo.h>
#include <Storages/KVStore/Decode/DecodedTiKVKeyValue.h>

#include <list>

namespace DB
{

using RegionDataReadInfo = std::tuple<RawTiDBPK, UInt8, Timestamp, std::shared_ptr<const TiKVValue>>;

using RegionDataReadInfoList = std::vector<RegionDataReadInfo>;

struct PrehandleResult
{
    std::vector<DM::ExternalDTFileInfo> ingest_ids;
    struct Stats
    {
        size_t parallels = 0;
        // These are bytes we actually read from sst reader.
        // It doesn't includes rocksdb's space amplification.
        size_t raft_snapshot_bytes = 0;
        size_t dt_disk_bytes = 0;
        size_t dt_total_bytes = 0;
        size_t total_keys = 0;
        size_t write_cf_keys = 0;
        size_t lock_cf_keys = 0;
        size_t default_cf_keys = 0;
        size_t max_split_write_cf_keys = 0;

        void mergeFrom(const Stats & other)
        {
            parallels += other.parallels;
            raft_snapshot_bytes += other.raft_snapshot_bytes;
            dt_disk_bytes += other.dt_disk_bytes;
            dt_total_bytes += other.dt_total_bytes;
            total_keys += other.total_keys;
            write_cf_keys += other.write_cf_keys;
            lock_cf_keys += other.lock_cf_keys;
            default_cf_keys += other.default_cf_keys;
            max_split_write_cf_keys = std::max(max_split_write_cf_keys, other.max_split_write_cf_keys);
        }
    };
    Stats stats;
};
} // namespace DB
