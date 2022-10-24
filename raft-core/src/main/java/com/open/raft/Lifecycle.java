
package com.open.raft;

/**
 * Service life cycle mark interface.
 *
 * 2018-Mar-12 3:47:04 PM
 */
public interface Lifecycle<T> {

    /**
     * Initialize the service.
     *
     * @return true when successes.
     */
    boolean init(final T opts);

    /**
     * Dispose the resources for service.
     */
    void shutdown();
}
