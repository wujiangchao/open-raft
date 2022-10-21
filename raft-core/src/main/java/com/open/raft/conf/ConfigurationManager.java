
package com.open.raft.conf;

import com.open.raft.util.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Configuration manager
 *
 */
public class ConfigurationManager {

    private static final Logger                  LOG            = LoggerFactory.getLogger(ConfigurationManager.class);

    private final LinkedList<ConfigurationEntry> configurations = new LinkedList<>();
    private ConfigurationEntry                   snapshot       = new ConfigurationEntry();

    /**
     * Adds a new conf entry.
     */
    public boolean add(final ConfigurationEntry entry) {
        if (!this.configurations.isEmpty()) {
            if (this.configurations.peekLast().getId().getIndex() >= entry.getId().getIndex()) {
                LOG.error("Did you forget to call truncateSuffix before the last log index goes back.");
                return false;
            }
        }
        return this.configurations.add(entry);
    }

    /**
     * [1, first_index_kept) are being discarded
     */
    public void truncatePrefix(final long firstIndexKept) {
        while (!this.configurations.isEmpty() && this.configurations.peekFirst().getId().getIndex() < firstIndexKept) {
            this.configurations.pollFirst();
        }
    }

    /**
     * (last_index_kept, infinity) are being discarded
     */
    public void truncateSuffix(final long lastIndexKept) {
        while (!this.configurations.isEmpty() && this.configurations.peekLast().getId().getIndex() > lastIndexKept) {
            this.configurations.pollLast();
        }
    }

    public ConfigurationEntry getSnapshot() {
        return this.snapshot;
    }

    public void setSnapshot(final ConfigurationEntry snapshot) {
        this.snapshot = snapshot;
    }

    public ConfigurationEntry getLastConfiguration() {
        if (this.configurations.isEmpty()) {
            return snapshot;
        } else {
            return this.configurations.peekLast();
        }
    }

    public ConfigurationEntry get(final long lastIncludedIndex) {
        if (this.configurations.isEmpty()) {
            Requires.requireTrue(lastIncludedIndex >= this.snapshot.getId().getIndex(),
                "lastIncludedIndex %d is less than snapshot index %d", lastIncludedIndex, this.snapshot.getId()
                    .getIndex());
            return this.snapshot;
        }
        ListIterator<ConfigurationEntry> it = this.configurations.listIterator();
        while (it.hasNext()) {
            if (it.next().getId().getIndex() > lastIncludedIndex) {
                it.previous();
                break;
            }
        }
        if (it.hasPrevious()) {
            // find the first position that is less than or equal to lastIncludedIndex.
            return it.previous();
        } else {
            // position not found position, return snapshot.
            return this.snapshot;
        }
    }
}
