#pragma once

#include <Storages/Transaction/ReadIndexWorker.h>
#include <kvproto/raft_serverpb.pb.h>

#include <ext/singleton.h>

namespace DB
{
kvrpcpb::ReadIndexRequest make_read_index_reqs(uint64_t region_id, uint64_t start_ts);

struct MockProxyRegion : MutexLockWrap
{
    raft_serverpb::RegionLocalState getState();

    raft_serverpb::RaftApplyState getApply();

    uint64_t getLatestCommitIndex();

    void updateCommitIndex(uint64_t index);

    MockProxyRegion();

    raft_serverpb::RegionLocalState state;
    raft_serverpb::RaftApplyState apply;
};

using MockProxyRegionPtr = std::shared_ptr<MockProxyRegion>;

struct MockAsyncNotifier
{
    RawCppPtr data; // notifier
    void (*wake_fn)(RawVoidPtr);
    void wake() const
    {
        wake_fn(data.ptr);
    }
    ~MockAsyncNotifier()
    {
        GcRawCppPtr(data.ptr, data.type);
    }
};

struct MockAsyncWaker
{
    std::shared_ptr<MockAsyncNotifier> data;
};

struct RawMockReadIndexTask : MutexLockWrap
{
    std::optional<kvrpcpb::ReadIndexResponse> poll(std::shared_ptr<MockAsyncNotifier> waker);

    void update(bool lock = false, bool region_error = false);

    kvrpcpb::ReadIndexRequest req;
    kvrpcpb::ReadIndexResponse resp;

    MockProxyRegionPtr region;
    std::shared_ptr<MockAsyncNotifier> waker;
    bool has_lock{false};
    bool has_region_error{false};
    bool finished{false};
};

struct MockReadIndexTask
{
    std::shared_ptr<RawMockReadIndexTask> data;
};

struct MockRaftStoreProxy : MutexLockWrap
{
    MockProxyRegionPtr getRegion(uint64_t id);

    MockProxyRegionPtr doGetRegion(uint64_t id);

    MockReadIndexTask * makeReadIndexTask(kvrpcpb::ReadIndexRequest req);

    void init(size_t region_num);

    size_t size() const;

    void wake();

    void testRunNormal(const std::atomic_bool & over);

    void runOneRound();

    void unsafeInvokeForTest(std::function<void(MockRaftStoreProxy &)> && cb);

    static TiFlashRaftProxyHelper SetRaftStoreProxyFFIHelper(RaftStoreProxyPtr);

    std::unordered_set<uint64_t> region_id_to_drop;
    std::unordered_set<uint64_t> region_id_to_error;
    std::map<uint64_t, MockProxyRegionPtr> regions;
    std::list<std::shared_ptr<RawMockReadIndexTask>> tasks;
    AsyncWaker::Notifier notifier;
};

enum class RawObjType : uint32_t
{
    None,
    MockReadIndexTask,
    MockAsyncWaker,
};

struct GCMonitor : MutexLockWrap
    , public ext::Singleton<GCMonitor>
{
    void add(RawObjType type, int64_t diff);

    bool checkClean();

    bool empty();

    std::unordered_map<RawObjType, int64_t> data;

    static GCMonitor global_gc_monitor;
};

} // namespace DB