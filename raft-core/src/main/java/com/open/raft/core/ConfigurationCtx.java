package com.open.raft.core;

/**
 * @Description TODO
 * @Date 2022/10/17 8:55
 * @Author jack wu
 */

import com.open.raft.Closure;
import com.open.raft.Status;
import com.open.raft.closure.CatchUpClosure;
import com.open.raft.conf.Configuration;
import com.open.raft.entity.PeerId;
import com.open.raft.error.RaftError;
import com.open.raft.util.Requires;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Configuration commit context.
 * 该类涉及member changes (增加、删除、修改节点、转移leader)
 */
public class ConfigurationCtx {

    private static final Logger LOG = LoggerFactory
            .getLogger(ConfigurationCtx.class);

    enum Stage {
        // none stage
        STAGE_NONE,
        //如果有追加或更换新节点，需要使新节点日志跟集群同步，复制完成日志后，调用catchUpClosure，下一步
        STAGE_CATCHING_UP, // the node is catching-up
        //将新旧配置复制到Follower，收到大部分回应后，下一步
        STAGE_JOINT,
        //通知Follower删除旧配置，收到大部分回应后，下一步 STAGE_NONE
        STAGE_STABLE
    }

    final NodeImpl node;
    Stage stage;
    /**
     * Peers change times
     */
    int nchanges;
    long version;
    List<PeerId> newPeers = new ArrayList<>();
    List<PeerId> oldPeers = new ArrayList<>();
    List<PeerId> addingPeers = new ArrayList<>();

    List<PeerId> newLearners = new ArrayList<>();
    List<PeerId> oldLearners = new ArrayList<>();
    Closure done;

    public ConfigurationCtx(final NodeImpl node) {
        super();
        this.node = node;
        this.stage = Stage.STAGE_NONE;
        this.version = 0;
        this.done = null;
    }

    /**
     * Start change configuration.
     */
    void start(final Configuration oldConf, final Configuration newConf, final Closure done) {
        if (isBusy()) {
            if (done != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Already in busy stage."));
            }
            throw new IllegalStateException("Busy stage");
        }
        if (this.done != null) {
            if (done != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Already have done closure."));
            }
            throw new IllegalArgumentException("Already have done closure");
        }
        this.done = done;
        this.stage = Stage.STAGE_CATCHING_UP;
        this.oldPeers = oldConf.listPeers();
        this.newPeers = newConf.listPeers();
        this.oldLearners = oldConf.listLearners();
        this.newLearners = newConf.listLearners();
        final Configuration adding = new Configuration();
        final Configuration removing = new Configuration();
        newConf.diff(oldConf, adding, removing);
        this.nchanges = adding.size() + removing.size();

        addNewLearners();
        //adding里面放的是当前的减去old,此处表示只删不增
        if (adding.isEmpty()) {
            nextStage();
            return;
        }
        addNewPeers(adding);
    }

    private void addNewPeers(final Configuration adding) {
        this.addingPeers = adding.listPeers();
        LOG.info("Adding peers: {}.", this.addingPeers);
        for (final PeerId newPeer : this.addingPeers) {
            if (!this.node.replicatorGroup.addReplicator(newPeer)) {
                LOG.error("Node {} start the replicator failed, peer={}.", this.node.getNodeId(), newPeer);
                //复制器启动异常，立即放弃追赶
                onCaughtUp(this.version, newPeer, false);
                return;
            }
            final OnCaughtUp caughtUp = new OnCaughtUp(this.node, this.node.currTerm, newPeer, this.version);
            final long dueTime = Utils.nowMs() + this.node.options.getElectionTimeoutMs();
            if (!this.node.replicatorGroup.waitCaughtUp(newPeer, this.node.options.getCatchupMargin(), dueTime,
                    caughtUp)) {
                LOG.error("Node {} waitCaughtUp, peer={}.", this.node.getNodeId(), newPeer);
                onCaughtUp(this.version, newPeer, false);
                return;
            }
        }
    }


    /**
     * Peer catch up callback
     *
     * @author boyan (boyan@alibaba-inc.com)
     * <p>
     * 2018-Apr-11 2:10:02 PM
     */
     static class OnCaughtUp extends CatchUpClosure {
        private final NodeImpl node;
        private final long term;
        private final PeerId peer;
        private final long version;

        public OnCaughtUp(final NodeImpl node, final long term, final PeerId peer, final long version) {
            super();
            this.node = node;
            this.term = term;
            this.peer = peer;
            this.version = version;
        }

        @Override
        public void run(final Status status) {
            this.node.onCaughtUp(this.peer, this.term, this.version, status);
        }
    }
    /**
     * 将Learners加入同步组
     */
    private void addNewLearners() {
        final Set<PeerId> addingLearners = new HashSet<>(this.newLearners);
        addingLearners.removeAll(this.oldLearners);
        LOG.info("Adding learners: {}.", addingLearners);
        for (final PeerId newLearner : addingLearners) {
            if (!this.node.replicatorGroup.addReplicator(newLearner, ReplicatorType.Learner)) {
                LOG.error("Node {} start the learner replicator failed, peer={}.", this.node.getNodeId(),
                        newLearner);
            }
        }
    }

    void onCaughtUp(final long version, final PeerId peer, final boolean success) {
        if (version != this.version) {
            LOG.warn("Ignore onCaughtUp message, mismatch configuration context version, expect {}, but is {}.",
                    this.version, version);
            return;
        }
        Requires.requireTrue(this.stage == Stage.STAGE_CATCHING_UP, "Stage is not in STAGE_CATCHING_UP");
        if (success) {
            this.addingPeers.remove(peer);
            if (this.addingPeers.isEmpty()) {
                nextStage();
                return;
            }
            return;
        }
        LOG.warn("Node {} fail to catch up peer {} when trying to change peers from {} to {}.",
                this.node.getNodeId(), peer, this.oldPeers, this.newPeers);
        reset(new Status(RaftError.ECATCHUP, "Peer %s failed to catch up.", peer));
    }

    void reset() {
        reset(null);
    }

    void reset(final Status st) {
        if (st != null && st.isOk()) {
            this.node.stopReplicator(this.newPeers, this.oldPeers);
            this.node.stopReplicator(this.newLearners, this.oldLearners);
        } else {
            this.node.stopReplicator(this.oldPeers, this.newPeers);
            this.node.stopReplicator(this.oldLearners, this.newLearners);
        }
        clearPeers();
        clearLearners();

        this.version++;
        this.stage = Stage.STAGE_NONE;
        this.nchanges = 0;
        if (this.done != null) {
            Utils.runClosureInThread(this.done, st != null ? st : new Status(RaftError.EPERM,
                    "Leader stepped down."));
            this.done = null;
        }
    }

    private void clearLearners() {
        this.newLearners.clear();
        this.oldLearners.clear();
    }

    private void clearPeers() {
        this.newPeers.clear();
        this.oldPeers.clear();
        this.addingPeers.clear();
    }

    /**
     * Invoked when this node becomes the leader, write a configuration change log as the first log.
     */
    void flush(final Configuration conf, final Configuration oldConf) {
        Requires.requireTrue(!isBusy(), "Flush when busy");
        this.newPeers = conf.listPeers();
        this.newLearners = conf.listLearners();
        if (oldConf == null || oldConf.isEmpty()) {
            this.stage = Stage.STAGE_STABLE;
            this.oldPeers = this.newPeers;
            this.oldLearners = this.newLearners;
        } else {
            this.stage = Stage.STAGE_JOINT;
            this.oldPeers = oldConf.listPeers();
            this.oldLearners = oldConf.listLearners();
        }
        this.node.unsafeApplyConfiguration(conf, oldConf == null || oldConf.isEmpty() ? null : oldConf, true);
    }

    void nextStage() {
        Requires.requireTrue(isBusy(), "Not in busy stage");
        switch (this.stage) {
            case STAGE_CATCHING_UP:
                if (this.nchanges > 0) {
                    this.stage = Stage.STAGE_JOINT;
                    this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners),
                            new Configuration(this.oldPeers), false);
                    return;
                }
            case STAGE_JOINT:
                this.stage = Stage.STAGE_STABLE;
                this.node.unsafeApplyConfiguration(new Configuration(this.newPeers, this.newLearners), null, false);
                break;
            case STAGE_STABLE:
                final boolean shouldStepDown = !this.newPeers.contains(this.node.serverId);
                reset(new Status());
                if (shouldStepDown) {
                    this.node.stepDown(this.node.currTerm, true, new Status(RaftError.ELEADERREMOVED,
                            "This node was removed."));
                }
                break;
            case STAGE_NONE:
                // noinspection ConstantConditions
                Requires.requireTrue(false, "Can't reach here");
                break;
        }
    }

    boolean isBusy() {
        return this.stage != Stage.STAGE_NONE;
    }
}
