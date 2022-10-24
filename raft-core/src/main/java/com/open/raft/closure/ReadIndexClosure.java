package com.open.raft.closure;

import com.open.raft.Closure;
import com.open.raft.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description TODO
 * @Date 2022/10/23 9:13
 * @Author jack wu
 */
public class ReadIndexClosure implements Closure {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReadIndexClosure.class);

    @Override
    public void run(Status status) {

    }
}
