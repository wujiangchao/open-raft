package com.open.raft.util.timer;


import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @Description Timer 接口提供了两个方法，分别是创建任务 newTimeout()
 * 和停止所有未执行任务 stop()。从方法的定义可以看出，Timer
 * 可以认为是上层的时间轮调度器，通过 newTimeout() 方法可以
 * 提交一个任务 TimerTask，并返回一个 Timeout
 * @Date 2022/11/17 11:12
 * @Author jack wu
 */
public interface Timer {
    /**
     * Schedules the specified {@link TimerTask} for one-time execution after
     * the specified delay.
     *
     * @return a handle which is associated with the specified task
     * @throws IllegalStateException      if this timer has been {@linkplain #stop() stopped} already
     * @throws RejectedExecutionException if the pending timeouts are too many and creating new timeout
     *                                    can cause instability in the system.
     */
    Timeout newTimeout(final TimerTask task, final long delay, final TimeUnit unit);

    /**
     * Releases all resources acquired by this {@link Timer} and cancels all
     * tasks which were scheduled but not executed yet.
     *
     * @return the handles associated with the tasks which were canceled by
     * this method
     */
    Set<Timeout> stop();
}
