
package com.open.raft.core;

/**
 * Replicator role
 *
 */
public enum ReplicatorType {
    Follower,
    /**
     * 且随着Quorum节点数目变多，在跨地域极高的网络延迟下，每次多数派达成一致的时间会很长，写效率很低。
     * 通过引入Learner(例如zk中Observer、etcd的raft learner[2])角色，即只进行数据同步而不参与多数派投票的角色，
     * 将写请求转发到某一个区域，来避免直接多节点部署的投票延时问题
     */
    Learner;

    public final boolean isFollower() {
        return this == Follower;
    }

    public final boolean isLearner() {
        return this == Learner;
    }
}