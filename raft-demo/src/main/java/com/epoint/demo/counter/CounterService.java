package com.epoint.demo.counter;

/**
 * @Description TODO
 * @Date 2022/10/31 17:15
 * @Author jack wu
 */
public interface CounterService {

    /**
     * Get current value from counter
     *
     * Provide consistent reading if {@code readOnlySafe} is true.
     */
    void get(final boolean readOnlySafe, final CounterClosure closure);

    /**
     * Add delta to counter then get value
     */
    void incrementAndGet(final long delta, final CounterClosure closure);
}
