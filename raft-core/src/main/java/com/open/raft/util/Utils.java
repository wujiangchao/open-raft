package com.open.raft.util;

import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * Gets the current monotonic time in milliseconds.
     */
    public static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
}
