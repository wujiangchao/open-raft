package com.open.raft;

import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.error.RaftException;
import com.open.raft.option.FSMCallerOptions;

/**
 * @Description TODO
 * @Date 2022/9/26 11:37
 * @Author jack wu
 */
public interface FSMCaller extends Lifecycle<FSMCallerOptions>{

    /**
     * Listen on lastAppliedLogIndex update events.
     *
     * @author dennis
     */
    interface LastAppliedLogIndexListener {

        /**
         * Called when lastAppliedLogIndex updated.
         *
         * @param lastAppliedLogIndex the log index of last applied
         */
        void onApplied(final long lastAppliedLogIndex);
    }


    /**
     * Called when stop following a leader.
     *
     * @param ctx context of leader change
     */
    boolean onStopFollowing(final LeaderChangeContext ctx);

    /**
     * Called when log entry committed
     *
     * @param committedIndex committed log index
     */
    boolean onCommitted(final long committedIndex);

    /**
     * Called when error happens.
     *
     * @param error error info
     */
    boolean onError(final RaftException error);

    /**
     * Returns the last log entry index to apply state machine.
     */
    long getLastAppliedIndex();

    /**
     * Called after saving snapshot.
     *
     * @param done callback
     */
    boolean onSnapshotSave(final SaveSnapshotClosure done);


}
