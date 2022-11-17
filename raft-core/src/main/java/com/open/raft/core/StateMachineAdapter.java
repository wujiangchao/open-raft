package com.open.raft.core;

import com.open.raft.Closure;
import com.open.raft.StateMachine;
import com.open.raft.Status;
import com.open.raft.conf.Configuration;
import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.error.RaftException;
import com.open.raft.storage.snapshot.SnapshotReader;
import com.open.raft.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description TODO
 * @Date 2022/11/16 17:33
 * @Author jack wu
 */
public abstract class StateMachineAdapter implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(StateMachineAdapter.class);

    @Override
    public void onShutdown() {
        LOG.info("onShutdown.");
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        error("onSnapshotSave");
        runClosure(done, "onSnapshotSave");
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        error("onSnapshotLoad", "while a snapshot is saved in " + reader.getPath());
        return false;
    }

    @Override
    public void onLeaderStart(final long term) {
        LOG.info("onLeaderStart: term={}.", term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        LOG.info("onLeaderStop: status={}.", status);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error(
                "Encountered an error={} on StateMachine {}, it's highly recommended to implement this method as raft stops working since some error occurs, you should figure out the cause and repair or remove this node.",
                e.getStatus(), getClassName(), e);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        LOG.info("onConfigurationCommitted: {}.", conf);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStopFollowing: {}.", ctx);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStartFollowing: {}.", ctx);
    }

    @SuppressWarnings("SameParameterValue")
    private void runClosure(final Closure done, final String methodName) {
        done.run(new Status(-1, "%s doesn't implement %s", getClassName(), methodName));
    }

    private String getClassName() {
        return getClass().getName();
    }

    @SuppressWarnings("SameParameterValue")
    private void error(final String methodName) {
        error(methodName, "");
    }

    private void error(final String methodName, final String msg) {
        LOG.error("{} doesn't implement {} {}.", getClassName(), methodName, msg);
    }
}
