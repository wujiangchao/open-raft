
package com.open.raft.rpc;

/**
 *
 * RPC connection
 * @author jiachun.fjc
 */
public interface Connection {

    /**
     * Get the attribute that bound to the connection.
     *
     * @param key the attribute key
     * @return the attribute value
     */
    Object getAttribute(final String key);

    /**
     * Set the attribute to the connection.
     *
     * @param key   the attribute key
     * @param value the attribute value
     */
    void setAttribute(final String key, final Object value);

    /**
     * Set the attribute to the connection if the key's item doesn't exist, otherwise returns the present item.
     *
     * @param key   the attribute key
     * @param value the attribute value
     * @return the previous value associated with the specified key, or
     *         <tt>null</tt> if there was no mapping for the key.
     */
    Object setAttributeIfAbsent(final String key, final Object value);

    /**
     * Close the connection.
     */
    void close();
}
