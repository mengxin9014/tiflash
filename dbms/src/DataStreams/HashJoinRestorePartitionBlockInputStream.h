// Copyright 2022 PingCAP, Ltd.
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

#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/Join.h>
namespace DB
{
class HashJoinRestorePartitionBlockInputStream : public IProfilingBlockInputStream
{
private:
    static constexpr auto name = "HashJoinRestore";

public:
    HashJoinRestorePartitionBlockInputStream(
        BlockInputStreams & build_inputs,
        BlockInputStreams & probe_inputs,
        UInt64 max_block_size,
        const JoinPtr & restore_join,
        const String & req_id)
    {
        size_t build_index = 0;
        for (auto & stream : build_inputs)
        {
            stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, restore_join, build_index++, req_id);
        }
        BlockInputStreamPtr build_stream = std::make_shared<UnionBlockInputStream<StreamUnionMode::Basic, /*ignore_block=*/true>>(build_inputs, BlockInputStreams{}, build_index, req_id);
        restore_join->initBuild(
            build_stream->getHeader(),
            build_inputs.size());

        size_t probe_index = 0;
        for (auto & stream : probe_inputs)
        {
            stream = std::make_shared<HashJoinProbeBlockInputStream>(stream, restore_join, probe_index++, req_id, max_block_size);
        }
        BlockInputStreamPtr probe_stream = std::make_shared<UnionBlockInputStream<StreamUnionMode::Basic, /*ignore_block=*/true>>(build_inputs, BlockInputStreams{}, probe_index, req_id);
        restore_join->initProbe(probe_stream->getHeader(), probe_inputs.size());
        build_stream->read();
        children.push_back(probe_stream);
    };

    String getName() const override { return name; }
    Block getHeader() const override { return children.back()->getHeader(); }

protected:
    Block readImpl() override { return children.back()->read(); }
};
} // namespace DB