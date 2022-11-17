
package com.open.raft.util;

import com.alipay.remoting.NamedThreadFactory;
import com.open.raft.util.timer.Timeout;
import com.open.raft.util.timer.Timer;
import com.open.raft.util.timer.TimerTask;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Repeatable timer based on java.util.Timer.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * <p>
 * 2018-Mar-30 3:45:37 PM
 */
public abstract class RepeatedTimer implements Describer {

    public static final Logger LOG = LoggerFactory.getLogger(RepeatedTimer.class);

    private final Lock lock = new ReentrantLock();
    /**
     * timer是HashedWheelTimer
     */
    private final Timer timer;
    /**
     * 实例是HashedWheelTimeout
     */
    private Timeout timeout;
    private boolean stopped;
    private volatile boolean running;
    private volatile boolean destroyed;
    private volatile boolean invoking;
    private volatile int timeoutMs;
    private final String name;

    public int getTimeoutMs() {
        return this.timeoutMs;
    }

    public RepeatedTimer(final String name, final int timeoutMs) {
        this(name, timeoutMs, new HashedWheelTimer(new NamedThreadFactory(name, true), 1, TimeUnit.MILLISECONDS, 2048));
    }

    public RepeatedTimer(final String name, final int timeoutMs, final Timer timer) {
        //name代表RepeatedTimer实例的种类，timeoutMs是超时时间
        super();
        this.name = name;
        this.timeoutMs = timeoutMs;
        this.stopped = true;
        this.timer = Requires.requireNonNull(timer, "timer");
    }

    /**
     * Subclasses should implement this method for timer trigger.
     */
    protected abstract void onTrigger();

    /**
     * Adjust timeoutMs before every scheduling.
     *
     * @param timeoutMs timeout millis
     * @return timeout millis
     */
    protected int adjustTimeout(final int timeoutMs) {
        return timeoutMs;
    }

    /**
     * 这个run方法会由timer进行回调，如果没有调用stop或destroy方法的话，
     * 那么调用完onTrigger方法后会继续调用schedule，然后一次次循环调用
     * RepeatedTimer的run方法。
     */
    public void run() {
        //表示RepeatedTimer已经被调用过
        this.invoking = true;
        try {
            onTrigger();
        } catch (final Throwable t) {
            LOG.error("Run timer failed.", t);
        }
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            this.invoking = false;
            //如果调用了stop方法，那么将不会继续调用schedule方法
            if (this.stopped) {
                this.running = false;
                invokeDestroyed = this.destroyed;
            } else {
                this.timeout = null;
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
        if (invokeDestroyed) {
            onDestroy();
        }
    }

    /**
     * Run the timer at once, it will cancel the timer and re-schedule it.
     */
    public void runOnceNow() {
        this.lock.lock();
        try {
            if (this.timeout != null && this.timeout.cancel()) {
                this.timeout = null;
                run();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Called after destroy timer.
     */
    protected void onDestroy() {
        LOG.info("Destroy timer: {}.", this);
    }

    /**
     * Start the timer.
     */
    public void start() {
        //上锁，只允许一个线程调用
        this.lock.lock();
        try {
            //destroyed默认是false
            if (this.destroyed) {
                return;
            }
            //stopped在构造器中初始化为ture
            if (!this.stopped) {
                return;
            }
            //启动完一次后下次就无法再次往下继续
            this.stopped = false;
            //running默认为false
            if (this.running) {
                return;
            }
            this.running = true;
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Restart the timer.
     * It will be started if it's stopped, and it will be restarted if it's running.
     *
     * @author Qing Wang (kingchin1218@gmail.com)
     * <p>
     * 2020-Mar-26 20:38:37 PM
     */
    public void restart() {
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.stopped = false;
            this.running = true;
            schedule();
        } finally {
            this.lock.unlock();
        }
    }

    private void schedule() {
        if (this.timeout != null) {
            //cancel the timeout if exsit
            this.timeout.cancel();
        }
        final TimerTask timerTask = timeout -> {
            try {
                RepeatedTimer.this.run();
            } catch (final Throwable t) {
                LOG.error("Run timer task failed, taskName={}.", RepeatedTimer.this.name, t);
            }
        };

        this.timeout = this.timer.newTimeout(timerTask, adjustTimeout(this.timeoutMs), TimeUnit.MILLISECONDS);
    }

    /**
     * Reset timer with new timeoutMs.
     *
     * @param timeoutMs timeout millis
     */
    public void reset(final int timeoutMs) {
        this.lock.lock();
        this.timeoutMs = timeoutMs;
        try {
            if (this.stopped) {
                return;
            }
            if (this.running) {
                schedule();
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Reset timer with current timeoutMs
     */
    public void reset() {
        this.lock.lock();
        try {
            reset(this.timeoutMs);
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * Destroy timer
     */
    public void destroy() {
        boolean invokeDestroyed = false;
        this.lock.lock();
        try {
            if (this.destroyed) {
                return;
            }
            this.destroyed = true;
            if (!this.running) {
                invokeDestroyed = true;
            }
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                if (this.timeout.cancel()) {
                    invokeDestroyed = true;
                    this.running = false;
                }
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
            this.timer.stop();
            if (invokeDestroyed) {
                onDestroy();
            }
        }
    }

    /**
     * Stop timer
     */
    public void stop() {
        this.lock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            if (this.timeout != null) {
                this.timeout.cancel();
                this.running = false;
                this.timeout = null;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        final String _describeString;
        this.lock.lock();
        try {
            _describeString = toString();
        } finally {
            this.lock.unlock();
        }
        out.print("  ") //
                .println(_describeString);
    }

    @Override
    public String toString() {
        return "RepeatedTimer{" + "timeout=" + this.timeout + ", stopped=" + this.stopped + ", running=" + this.running
                + ", destroyed=" + this.destroyed + ", invoking=" + this.invoking + ", timeoutMs=" + this.timeoutMs
                + ", name='" + this.name + '\'' + '}';
    }
}
