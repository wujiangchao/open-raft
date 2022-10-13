package com.open.raft.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description TODO
 * @Date 2022/10/11 9:15
 * @Author jack wu
 */
public class ThreadId {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadId.class);
    private final Object data;
    private final ReentrantLock lock = new ReentrantLock();
    private final OnError onError;
    private volatile boolean destroyed;

    public ThreadId(Object data, OnError onError) {
        this.data = data;
        this.onError = onError;
        this.destroyed = false;
    }

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
     *
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

    public Object lock() {
        if (this.destroyed) {
            return null;
        }
        this.lock.lock();
        // Got the lock, double checking state.
        if (this.destroyed) {
            // should release lock
            this.lock.unlock();
            return null;
        }
        return this.data;
    }
}
