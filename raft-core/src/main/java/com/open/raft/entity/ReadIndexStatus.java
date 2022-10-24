
package com.open.raft.entity;

import com.open.raft.rpc.RpcRequests;

import java.util.List;

/**
 * ReadIndex requests statuses.
 *
 * @author dennis
 */
public class ReadIndexStatus {

    /**
     * raw request
     */
    private final RpcRequests.ReadIndexRequest request;
    /**
     * read index requests in batch.
     */
    private final List<ReadIndexState> states;
    /**
     * committed log index.
     */
    private final long index;

    public ReadIndexStatus(List<ReadIndexState> states, RpcRequests.ReadIndexRequest request, long index) {
        super();
        this.index = index;
        this.request = request;
        this.states = states;
    }

    public boolean isApplied(long appliedIndex) {
        return appliedIndex >= this.index;
    }

    public boolean isOverMaxReadIndexLag(long applyIndex, int maxReadIndexLag) {
        if (maxReadIndexLag < 0) {
            return false;
        }
        return this.index - applyIndex > maxReadIndexLag;
    }

    public long getIndex() {
        return index;
    }

    public RpcRequests.ReadIndexRequest getRequest() {
        return request;
    }

    public List<ReadIndexState> getStates() {
        return states;
    }

}