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

#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/HashJoinRestorePartitionBlockInputStream.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    size_t probe_index_,
    const String & req_id,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , join(join_)
    , probe_index(probe_index_)
    , max_block_size(max_block_size_)
    , probe_process_info(max_block_size_)
    , squashing_transform(max_block_size_)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
    if (join->needReturnNonJoinedData())
        non_joined_stream = join->createStreamWithNonJoinedRows(input->getHeader(), probe_index, join->getProbeConcurrency(), max_block_size);
}

Block HashJoinProbeBlockInputStream::getTotals()
{
    if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        if (!totals)
        {
            if (join->hasTotals())
            {
                for (const auto & name_and_type : child->getHeader().getColumnsWithTypeAndName())
                {
                    auto column = name_and_type.type->createColumn();
                    column->insertDefault();
                    totals.insert(ColumnWithTypeAndName(std::move(column), name_and_type.type, name_and_type.name));
                }
            }
            else
                return totals; /// There's nothing to JOIN.
        }
        join->joinTotals(totals);
    }

    return totals;
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    assert(res.rows() == 0);
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(res));
    return join->joinBlock(header_probe_process_info);
}

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    if (non_joined_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    std::cout << "read probe : " << probe_index << std::endl
              << std::flush;
    // if join finished, return {} directly.
    if (squashing_transform.isJoinFinished())
    {
        return Block{};
    }

    while (squashing_transform.needAppendBlock())
    {
        Block result_block = getOutputBlock();
        squashing_transform.appendBlock(result_block);
    }
    auto ret = squashing_transform.getFinalOutputBlock();
    return ret;
}

void HashJoinProbeBlockInputStream::readSuffixImpl()
{
    LOG_DEBUG(log, "Finish join probe, total output rows {}, joined rows {}, non joined rows {}", joined_rows + non_joined_rows, joined_rows, non_joined_rows);
}

Block HashJoinProbeBlockInputStream::getOutputBlock()
{
    while (true)
    {
        switch (status)
        {
        case ProbeStatus::PROBE:
        {
            if (probe_process_info.all_rows_joined_finish)
            {
                Block block = join->getOneProbeBlockWithLock();

                if (!block)
                {
                    block = children.back()->read();
                    if (block)
                    {
                        join->dispatchProbeBlock(block);
                        break;
                    }

                    join->finishOneProbe();
                    std::cout << "finish probe, index : " << probe_index << " round : " << join->restore_round << std::endl
                              << std::flush;
                    restore_probe_stream.reset();

                    if (join->needReturnNonJoinedData())
                        status = ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA;
                    else if (join->hasPartitionSpilledWithLock())
                    {
                        std::cout << "PROBE ->  WAIT_SPILL_FINISHED, index : " << probe_index << " round : " << join->restore_round << std::endl
                                  << std::flush;
                        status = ProbeStatus::WAIT_SPILL_FINISHED;
                    }
                    else
                    {
                        std::cout << "PROBE ->  FINISHED, index : " << probe_index << " round : " << join->restore_round << std::endl
                                  << std::flush;
                        status = ProbeStatus::FINISHED;
                    }
                    break;
                }
                else
                {
                    join->checkTypes(block);
                    probe_process_info.resetBlock(std::move(block));
                }
            }
            auto ret = join->joinBlock(probe_process_info);
            joined_rows += ret.rows();
            return ret;
        }
        case ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA:
            join->waitUntilAllProbeFinished();
            status = ProbeStatus::READ_NON_JOINED_DATA;
            non_joined_stream->readPrefix();
            break;
        case ProbeStatus::READ_NON_JOINED_DATA:
        {
            auto block = non_joined_stream->read();
            non_joined_rows += block.rows();
            if (!block)
            {
                non_joined_stream->readSuffix();
                if (join->getTotalSpilledPartitions() > 0)
                {
                    status = ProbeStatus::BUILD_RESTORE_PARTITION;
                }
                else
                {
                    status = ProbeStatus::FINISHED;
                }
                break;
            }
            return block;
        }
        case ProbeStatus::WAIT_SPILL_FINISHED:
        {
            std::cout << "WAIT_SPILL_FINISHED ->  BUILD_RESTORE_PARTITION, index : " << probe_index << " round : " << join->restore_round << std::endl
                      << std::flush;
            join->waitUntilAllProbeFinished();
            status = ProbeStatus::BUILD_RESTORE_PARTITION;
            break;
        }
        case ProbeStatus::BUILD_RESTORE_PARTITION:
        {
            std::cout << "BUILD_RESTORE_PARTITION ->  WAIT_BUILD_RESTORE_PARTITION_FINISHED, index : " << probe_index << " round : " << join->restore_round << std::endl
                      << std::flush;
            auto [restore_join, stream_index, build_stream, probe_stream] = join->getOneRestoreStream();
            //            join = restore_join;
            build_stream = std::make_shared<HashJoinBuildBlockInputStream>(build_stream, restore_join, stream_index, log->identifier());
            restore_probe_stream = std::make_shared<HashJoinProbeBlockInputStream>(probe_stream, restore_join, stream_index, log->identifier(), max_block_size);
            while (build_stream->read())
                ;
            status = ProbeStatus::WAIT_BUILD_RESTORE_PARTITION_FINISHED;
            break;
        }
        case ProbeStatus::WAIT_BUILD_RESTORE_PARTITION_FINISHED:
        {
            std::cout << "WAIT_BUILD_RESTORE_PARTITION_FINISHED ->  PROBE_RESTORE_PARTITION, index : " << probe_index << " round : " << join->restore_round << std::endl
                      << std::flush;
            join->waitUntilAllBuildFinished();
            probe_process_info.all_rows_joined_finish = true;
            status = ProbeStatus::PROBE_RESTORE_PARTITION;
            break;
        }
        case ProbeStatus::PROBE_RESTORE_PARTITION:
        {
            Block ret = restore_probe_stream->read();
            if (likely(ret))
            {
                return ret;
            }
            //            assert(join->getParent());
            //            join = join->getParent();
            status = join->hasPartitionSpilledWithLock() || join->hasRestoreStreamsWithLock() ? ProbeStatus::BUILD_RESTORE_PARTITION : ProbeStatus::FINISHED;

            if (status == ProbeStatus::BUILD_RESTORE_PARTITION)
            {
                std::cout << "PROBE_RESTORE_PARTITION ->  BUILD_RESTORE_PARTITION, index : " << probe_index << " round : " << join->restore_round << std::endl
                          << std::flush;
            }
            else
            {
                std::cout << "PROBE_RESTORE_PARTITION ->  FINISHED, index : " << probe_index << " round : " << join->restore_round << std::endl
                          << std::flush;
            }
            break;
        }
        case ProbeStatus::FINISHED:
            return {};
        }
    }
}

} // namespace DB
