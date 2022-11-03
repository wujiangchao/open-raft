package com.open.raft.closure;

import com.open.raft.Status;
import com.open.raft.core.ReadOnlyServiceImpl;
import com.open.raft.entity.ReadIndexState;
import com.open.raft.entity.ReadIndexStatus;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosureAdapter;
import com.open.raft.util.Bytes;
import com.open.raft.util.Utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Date 2022/10/24 15:36
 * @Author jack wu
 */
public /**
 * ReadIndexResponse process closure
 *
 * @author dennis
 */
class ReadIndexResponseClosure extends RpcResponseClosureAdapter<RpcRequests.ReadIndexResponse> {

    final List<ReadIndexState> states;
    final RpcRequests.ReadIndexRequest request;
    final ReadOnlyServiceImpl readOnlyService;

    public ReadIndexResponseClosure(final List<ReadIndexState> states, final RpcRequests.ReadIndexRequest request, ReadOnlyServiceImpl readOnlyService) {
        super();
        this.states = states;
        this.request = request;
        this.readOnlyService = readOnlyService;
    }

    /**
     * Called when ReadIndex response returns.
     * <p>
     * 这里会等待向Leader发送的读请求的返回。
     * 注意，由于在 Follower 发起的读请求，最终需要从 Leader 获取 Leader 当前最新的 commited index
     * 位点信息，并且需要等待自己的状态机回放日志到该位点，因此，这一段的时间是不可知的，有可能需要等待很长的
     * 时间，因此建议使用者最好加上读取超时时间，然后采用 Leader 读作为兜底机制。尽可能的让读取数据操作
     * 成功。
     */
    @Override
    public void run(final Status status) {
        // 如果读失败，通知所有读请求的回调告知失败。
        if (!status.isOk()) {
            notifyFail(status);
            return;
        }
        final RpcRequests.ReadIndexResponse readIndexResponse = getResponse();
        if (!readIndexResponse.getSuccess()) {
            notifyFail(new Status(-1, "Fail to run ReadIndex task, maybe the leader stepped down."));
            return;
        }
        // Success
        // 获取当前Leader返回的 index 位点信息
        final ReadIndexStatus readIndexStatus = new ReadIndexStatus(this.states, this.request,
                readIndexResponse.getIndex());
        for (final ReadIndexState state : this.states) {
            // Records current commit log index.
            state.setIndex(readIndexResponse.getIndex());
        }

        boolean doUnlock = true;
        readOnlyService.lock.lock();
        try {
            //判断commitindex 是否被apply
            if (readIndexStatus.isApplied(readOnlyService.fsmCaller.getLastAppliedIndex())) {
                // 如果当前的状态机回放日志的位点已经达到或者超过了 Leader 回传的 Index，
                // 则通知所有线性读回调函数可以正确执行了。
                // Already applied, notify readIndex request.
                readOnlyService.lock.unlock();
                doUnlock = false;
                notifySuccess(readIndexStatus);
            } else {
                //当前节点的applyindex 和 leader节点的commitindex相差太多
                if (readIndexStatus.isOverMaxReadIndexLag(ReadreadOnlyService.fsmCaller.getLastAppliedIndex(), ReadreadOnlyService.raftOptions.getMaxReadIndexLag())) {
                    ReadreadOnlyService.lock.unlock();
                    doUnlock = false;
                    notifyFail(new Status(-1, "Fail to run ReadIndex task, the gap of current node's apply index between leader's commit index over maxReadIndexLag"));
                } else {
                    // Not applied, add it to pending-notify cache.
                    // 如果还没有回放到对应的日志位点的话，需要将当前的信息进行暂存在一个队列中。
                    ReadreadOnlyService.pendingNotifyStatus
                            .computeIfAbsent(readIndexStatus.getIndex(), k -> new ArrayList<>(10))
                            .add(readIndexStatus);
                }
            }
        } finally {
            if (doUnlock) {
                ReadreadOnlyService.lock.unlock();
            }
        }
    }

    private void notifyFail(final Status status) {
        final long nowMs = Utils.monotonicMs();
        for (final ReadIndexState state : this.states) {
            ReadreadOnlyService.nodeMetrics.recordLatency("read-index", nowMs - state.getStartTimeMs());
            final ReadIndexClosure done = state.getDone();
            if (done != null) {
                final Bytes reqCtx = state.getRequestContext();
                done.run(status, ReadIndexClosure.INVALID_LOG_INDEX, reqCtx != null ? reqCtx.get() : null);
            }
        }
    }

    private void notifySuccess(final ReadIndexStatus status) {
        final long nowMs = Utils.monotonicMs();
        final List<ReadIndexState> states = status.getStates();
        final int taskCount = states.size();
        for (int i = 0; i < taskCount; i++) {
            final ReadIndexState task = states.get(i);
            final ReadIndexClosure done = task.getDone(); // stack copy
            if (done != null) {
                this.nodeMetrics.recordLatency("read-index", nowMs - task.getStartTimeMs());
                done.setResult(task.getIndex(), task.getRequestContext().get());
                done.run(Status.OK());
            }
        }
    }
}