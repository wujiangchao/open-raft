package com.open.raft.util.timer;


/**
 * @Description 定时器工厂接口
 * @Date 2022/11/17 11:09
 * @Author jack wu
 */
public interface RaftTimerFactory {
    /**
     * 选举定时器获取
     *
     * @param shared
     * @param name
     * @return
     */
    Timer getElectionTimer(final boolean shared, final String name);

    /**
     * 投票定时器获取
     *
     * @param shared
     * @param name
     * @return
     */
    Timer getVoteTimer(final boolean shared, final String name);

    /**
     * 下台定时器获取
     *
     * @param shared
     * @param name
     * @return
     */
    Timer getStepDownTimer(final boolean shared, final String name);

    /**
     * 快照定时器获取
     *
     * @param shared
     * @param name
     * @return
     */
    Timer getSnapshotTimer(final boolean shared, final String name);
}
