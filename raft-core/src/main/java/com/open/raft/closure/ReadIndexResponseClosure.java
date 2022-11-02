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
     */
    @Override
    public void run(final Status status) {
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
                // Already applied, notify readIndex request.
                readOnlyService.lock.unlock();
                doUnlock = false;
                notifySuccess(readIndexStatus);
            } else {
                if (readIndexStatus.isOverMaxReadIndexLag(ReadreadOnlyService.fsmCaller.getLastAppliedIndex(), ReadreadOnlyService.raftOptions.getMaxReadIndexLag())) {
                    ReadreadOnlyService.lock.unlock();
                    doUnlock = false;
                    notifyFail(new Status(-1, "Fail to run ReadIndex task, the gap of current node's apply index between leader's commit index over maxReadIndexLag"));
                } else {
                    // Not applied, add it to pending-notify cache.
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
}