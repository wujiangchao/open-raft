package com.open.raft.conf;

import com.open.raft.entity.PeerId;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * @Description TODO
 * @Date 2022/10/9 15:13
 * @Author jack wu
 */
public class Configuration implements Iterable<PeerId>{


    @Override
    public Iterator<PeerId> iterator() {
        return null;
    }
}
