package com.open.raft.util;

import com.alipay.remoting.NamedThreadFactory;
import com.open.raft.Closure;
import com.open.raft.Status;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description TODO
 * @Date 2022/9/22 20:18
 * @Author jack wu
 */
public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    /**
     * The configured number of available processors. The default is
     * {@link Runtime#availableProcessors()}. This can be overridden by setting the system property
     * "jraft.available_processors".
     */
    public static final int CPUS = SystemPropertyUtil.getInt(
            "raft.available_processors", Runtime
                    .getRuntime().availableProcessors());

    /**
     * Default jraft closure executor pool minimum size, CPUs by default.
     */
    public static final int           MIN_CLOSURE_EXECUTOR_POOL_SIZE      = SystemPropertyUtil.getInt(
            "raft.closure.threadpool.size.min",
            CPUS);

    /**
     * Default jraft closure executor pool maximum size.
     */
    public static final int           MAX_CLOSURE_EXECUTOR_POOL_SIZE      = SystemPropertyUtil.getInt(
            "raft.closure.threadpool.size.max",
            Math.max(100, CPUS * 5));

    /**
     * Gets the current monotonic time in milliseconds.
     */
    public static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }


    /**
     * Global thread pool to run closure.
     */
    private static ThreadPoolExecutor CLOSURE_EXECUTOR                    = ThreadPoolUtil
            .newBuilder()
            .poolName("RAFT_CLOSURE_EXECUTOR")
            .enableMetric(true)
            .coreThreads(
                    MIN_CLOSURE_EXECUTOR_POOL_SIZE)
            .maximumThreads(
                    MAX_CLOSURE_EXECUTOR_POOL_SIZE)
            .keepAliveSeconds(60L)
            .workQueue(new SynchronousQueue<>())
            .threadFactory(
                    new NamedThreadFactory(
                            "Raft-Closure-Executor-", true))
            .build();

    /**
     * Run a task in thread pool,returns the future object.
     */
    public static Future<?> runInThread(final Runnable runnable) {
        return CLOSURE_EXECUTOR.submit(runnable);
    }

    /**
     * Run closure with status in thread pool.
     */
    @SuppressWarnings("Convert2Lambda")
    public static Future<?> runClosureInThread(final Closure done, final Status status) {
        if (done == null) {
            return null;
        }

        return runInThread(new Runnable() {

            @Override
            public void run() {
                try {
                    done.run(status);
                } catch (final Throwable t) {
                    LOG.error("Fail to run done closure", t);
                }
            }
        });
    }

}
