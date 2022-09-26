package com.open.raft;

import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.option.FSMCallerOptions;

/**
 * @Description TODO
 * @Date 2022/9/26 11:37
 * @Author jack wu
 */
public interface FSMCaller extends Lifecycle<FSMCallerOptions>{

    /**
     * Called when stop following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStopFollowing(final LeaderChangeContext ctx);
}