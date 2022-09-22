package com.open.raft.option;


import com.open.raft.RaftServiceFactory;
import com.open.raft.core.ElectionPriority;
import com.open.raft.util.Copiable;
import com.open.raft.util.RaftServiceLoader;
import com.open.raft.util.Utils;

import javax.security.auth.login.Configuration;


/**
 * @Date 2022/7/8 10:11
 * @Author jack wu
 */
public class NodeOptions extends RpcOptions implements Copiable<NodeOptions> {


    public static final RaftServiceFactory defaultServiceFactory = RaftServiceLoader.load(RaftServiceFactory.class)
            .first();

    /**
     * A follower would become a candidate if it doesn't receive any message
     * from the leader in |election_timeout_ms| milliseconds
     * Default: 1000 (1s)
     */
    private int electionTimeoutMs = 1000;

    /**
     * CLI 服务就是 Client CommandLine Service，是 jraft 在 raft group 节点提供的 RPC
     * 服务中暴露了一系列用于管理 raft group 的服务接口，例如增加节点、移除节点、改变节点配置列表、
     * 重置节点配置以及转移 leader 等功能
     * <p>
     * 是否关闭 Cli 服务,默认不关闭
     */
    private boolean disableCli = false;

    /**
     * A snapshot saving would be triggered every |snapshot_interval_s| seconds
     * if this was reset as a positive number
     * If |snapshot_interval_s| <= 0, the time based snapshot would be disabled.
     * Default: 3600 (1 hour)
     */
    private int snapshotIntervalSecs = 3600;


    /**
     * If node is starting from a empty environment (both LogStorage and
     * SnapshotStorage are empty), it would use |initial_conf| as the
     * configuration of the group, otherwise it would load configuration from
     * the existing environment.
     * Default: A empty group
     */
//    private Configuration initialConf = new Configuration();

    /**
     * The specific StateMachine implemented your business logic, which must be a valid instance.
     */
//    private StateMachine fsm;

    /**
     * Describe a specific LogStorage in format ${type}://${parameters}
     */
    private String logUri;

    /**
     * Describe a specific RaftMetaStorage in format ${type}://${parameters}
     */
    private String raftMetaUri;

    /**
     * Describe a specific SnapshotStorage in format ${type}://${parameters}
     */
    private String snapshotUri;

    /**
     * RAFT request RPC executor pool size, use default executor if -1.
     */
    private int raftRpcThreadPoolSize = Utils.CPUS * 6;

    /**
     * CLI service request RPC executor pool size, use default executor if -1.
     */
    private int cliRpcThreadPoolSize = Utils.CPUS;

    /**
     * Whether to enable metrics for node.
     */
    private boolean enableMetrics = false;

    /**
     * One node's local priority value would be set to | electionPriority |
     * value when it starts up.If this value is set to 0,the node will never be a leader.
     * If this node doesn't support priority election,then set this value to -1.
     * Default: -1
     */
    private int electionPriority = ElectionPriority.Disabled;


    /**
     * Whether use global timer pool, if true, the {@code timerPoolSize} will be invalid.
     */
    private boolean sharedTimerPool = false;


    /**
     * Timer manager thread pool size
     */
    private int timerPoolSize = Utils.CPUS * 3 > 20 ? 20 : Utils.CPUS * 3;

    private RaftOptions raftOptions = new RaftOptions();

    /**
     * Whether use global election timer
     */
    private boolean sharedElectionTimer = false;
    /**
     * Whether use global vote timer
     */
    private boolean sharedVoteTimer = false;
    /**
     * Whether use global step down timer
     */
    private boolean sharedStepDownTimer = false;
    /**
     * Whether use global snapshot timer
     */
    private boolean sharedSnapshotTimer = false;

    /**
     * 将新的节点添加到_replicator_group里面，等到新节点的日志追赶成功就调用回调进入下一个stage，
     * 如果超时还没有赶上，并且节点还存活的话就重试。是否追赶上的判断标志是新加入节点和leader之间的
     * log index的差距小于catchup_margin
     */
    private int catchupMargin = 1000;


    /**
     * Leader lease time's ratio of electionTimeoutMs,
     * To minimize the effects of clock drift, we should make that:
     * clockDrift + leaderLeaseTimeoutMs < electionTimeout
     * Default: 90, Max: 100
     */
    private int leaderLeaseTimeRatio = 90;

    /**
     * Apply task in blocking or non-blocking mode, ApplyTaskMode.NonBlocking by default.
     */
    private ApplyTaskMode applyTaskMode = ApplyTaskMode.NonBlocking;

    /**
     * if enable, we will filter duplicate files before copy remote snapshot,
     * to avoid useless transmission. Two files in local and remote are duplicate,
     * only if they has the same filename and the same checksum (stored in file meta).
     * Default: false
     */
    private boolean filterBeforeCopyRemote = false;

    /**
     * 配置 SnapshotThrottle，SnapshotThrottle 用于重盘读/写场景限流的，好比磁盘读写、网络带宽。
     * If non-null, we will pass this SnapshotThrottle to SnapshotExecutor
     * Default: NULL
     */
    //private SnapshotThrottle snapshotThrottle;


    /**
     * A snapshot saving would be triggered every |snapshot_interval_s| seconds,
     * and at this moment when state machine's lastAppliedIndex value
     * minus lastSnapshotId value is greater than snapshotLogIndexMargin value,
     * the snapshot action will be done really.
     * If |snapshotLogIndexMargin| <= 0, the distance based snapshot would be disable.
     * Default: 0
     */
    private int snapshotLogIndexMargin = 0;


    /**
     * 优先级衰减的间隔值，如果用户认为 Node 节点本身的 priority 值衰减过慢，
     * 可以适当地增加该配置参数，这样可以使得 priority 值较小的节点不需要花费太多时间即可完成衰减；
     * If next leader is not elected until next election timeout, it exponentially
     * decay its local target priority, for example target_priority = target_priority - gap
     * Default: 10
     */
    private int decayPriorityGap = 10;

    public RaftServiceFactory getServiceFactory() {
        return serviceFactory;
    }

    public void setServiceFactory(RaftServiceFactory serviceFactory) {
        this.serviceFactory = serviceFactory;
    }

    private RaftServiceFactory serviceFactory = defaultServiceFactory;


    public RaftOptions getRaftOptions() {
        return raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

//    public Configuration getInitialConf() {
//        return initialConf;
//    }
//
//    public void setInitialConf(Configuration initialConf) {
//        this.initialConf = initialConf;
//    }

    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public boolean isDisableCli() {
        return disableCli;
    }

    public void setDisableCli(boolean disableCli) {
        this.disableCli = disableCli;
    }

    public int getSnapshotIntervalSecs() {
        return snapshotIntervalSecs;
    }

    public void setSnapshotIntervalSecs(int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

//    public StateMachine getFsm() {
//        return fsm;
//    }
//
//    public void setFsm(StateMachine fsm) {
//        this.fsm = fsm;
//    }

    public String getLogUri() {
        return logUri;
    }

    public void setLogUri(String logUri) {
        this.logUri = logUri;
    }

    public String getRaftMetaUri() {
        return raftMetaUri;
    }

    public void setRaftMetaUri(String raftMetaUri) {
        this.raftMetaUri = raftMetaUri;
    }

    public String getSnapshotUri() {
        return snapshotUri;
    }

    public void setSnapshotUri(String snapshotUri) {
        this.snapshotUri = snapshotUri;
    }

    public int getRaftRpcThreadPoolSize() {
        return raftRpcThreadPoolSize;
    }

    public void setRaftRpcThreadPoolSize(int raftRpcThreadPoolSize) {
        this.raftRpcThreadPoolSize = raftRpcThreadPoolSize;
    }

    public int getCliRpcThreadPoolSize() {
        return cliRpcThreadPoolSize;
    }

    public void setCliRpcThreadPoolSize(int cliRpcThreadPoolSize) {
        this.cliRpcThreadPoolSize = cliRpcThreadPoolSize;
    }


    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }

    public int getElectionPriority() {
        return electionPriority;
    }

    public void setElectionPriority(int electionPriority) {
        this.electionPriority = electionPriority;
    }

    public boolean isSharedTimerPool() {
        return sharedTimerPool;
    }

    public void setSharedTimerPool(boolean sharedTimerPool) {
        this.sharedTimerPool = sharedTimerPool;
    }

    public int getTimerPoolSize() {
        return timerPoolSize;
    }

    public void setTimerPoolSize(int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
    }


    public boolean isSharedElectionTimer() {
        return sharedElectionTimer;
    }

    public void setSharedElectionTimer(boolean sharedElectionTimer) {
        this.sharedElectionTimer = sharedElectionTimer;
    }

    public boolean isSharedVoteTimer() {
        return sharedVoteTimer;
    }

    public void setSharedVoteTimer(boolean sharedVoteTimer) {
        this.sharedVoteTimer = sharedVoteTimer;
    }

    public boolean isSharedStepDownTimer() {
        return sharedStepDownTimer;
    }

    public void setSharedStepDownTimer(boolean sharedStepDownTimer) {
        this.sharedStepDownTimer = sharedStepDownTimer;
    }

    public boolean isSharedSnapshotTimer() {
        return sharedSnapshotTimer;
    }

    public void setSharedSnapshotTimer(boolean sharedSnapshotTimer) {
        this.sharedSnapshotTimer = sharedSnapshotTimer;
    }

    public int getCatchupMargin() {
        return catchupMargin;
    }

    public void setCatchupMargin(int catchupMargin) {
        this.catchupMargin = catchupMargin;
    }

    public int getLeaderLeaseTimeoutMs() {
        return this.electionTimeoutMs * this.leaderLeaseTimeRatio / 100;
    }

    public ApplyTaskMode getApplyTaskMode() {
        return applyTaskMode;
    }

    public void setApplyTaskMode(ApplyTaskMode applyTaskMode) {
        this.applyTaskMode = applyTaskMode;
    }

    public boolean isFilterBeforeCopyRemote() {
        return filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

//    public SnapshotThrottle getSnapshotThrottle() {
//        return snapshotThrottle;
//    }
//
//    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
//        this.snapshotThrottle = snapshotThrottle;
//    }

    public int getSnapshotLogIndexMargin() {
        return snapshotLogIndexMargin;
    }

    public void setSnapshotLogIndexMargin(int snapshotLogIndexMargin) {
        this.snapshotLogIndexMargin = snapshotLogIndexMargin;
    }

    public int getDecayPriorityGap() {
        return decayPriorityGap;
    }

    public void setDecayPriorityGap(int decayPriorityGap) {
        this.decayPriorityGap = decayPriorityGap;
    }

    @Override
    public NodeOptions copy() {
        final NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        nodeOptions.setElectionPriority(this.electionPriority);
        nodeOptions.setDecayPriorityGap(this.decayPriorityGap);
        nodeOptions.setSnapshotIntervalSecs(this.snapshotIntervalSecs);
        nodeOptions.setSnapshotLogIndexMargin(this.snapshotLogIndexMargin);
        nodeOptions.setCatchupMargin(this.catchupMargin);
        nodeOptions.setFilterBeforeCopyRemote(this.filterBeforeCopyRemote);
        nodeOptions.setDisableCli(this.disableCli);
        nodeOptions.setSharedTimerPool(this.sharedTimerPool);
        nodeOptions.setTimerPoolSize(this.timerPoolSize);
        nodeOptions.setCliRpcThreadPoolSize(this.cliRpcThreadPoolSize);
        nodeOptions.setRaftRpcThreadPoolSize(this.raftRpcThreadPoolSize);
        nodeOptions.setEnableMetrics(this.enableMetrics);
        nodeOptions.setRaftOptions(this.raftOptions == null ? new RaftOptions() : this.raftOptions.copy());
        nodeOptions.setSharedElectionTimer(this.sharedElectionTimer);
        nodeOptions.setSharedVoteTimer(this.sharedVoteTimer);
        nodeOptions.setSharedStepDownTimer(this.sharedStepDownTimer);
        nodeOptions.setSharedSnapshotTimer(this.sharedSnapshotTimer);
        return nodeOptions;
    }
}
