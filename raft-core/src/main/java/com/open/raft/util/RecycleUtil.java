
package com.open.raft.util;

/**
 * Recycle tool for {@link com.open.raft.util.Recyclable}.
 *
 */
public final class RecycleUtil {

    /**
     * Recycle designated instance.
     */
    public static boolean recycle(final Object obj) {
        return obj instanceof Recyclable && ((Recyclable) obj).recycle();
    }

    private RecycleUtil() {
    }
}
