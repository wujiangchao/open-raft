package com.open.raft.util;

import com.open.raft.core.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @Description TODO
 * @Date 2022/10/11 9:15
 * @Author jack wu
 */
public class ThreadId {
    private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);


    public interface OnError {

        /**
         * Error callback, it will be called in lock.
         *
         * @param id        the thread id
         * @param data      the data
         * @param errorCode the error code
         */
        void onError(final ThreadId id, final Object data, final int errorCode);
    }

    /**
     * Set error code, run the onError callback
     * with code immediately in lock.
     * @param errorCode error code
     */
    public void setError(final int errorCode) {
        if (this.destroyed) {
            LOG.warn("ThreadId: {} already destroyed, ignore error code: {}", this.data, errorCode);
            return;
        }
        this.lock.lock();
        try {
            if (this.destroyed) {
                LOG.warn("ThreadId: {} already destroyed, ignore error code: {}", this.data, errorCode);
                return;
            }
            if (this.onError != null) {
                this.onError.onError(this, this.data, errorCode);
            }

        } finally {
            // Maybe destroyed in callback
            if (!this.destroyed) {
                this.lock.unlock();
            }
        }
    }

}
