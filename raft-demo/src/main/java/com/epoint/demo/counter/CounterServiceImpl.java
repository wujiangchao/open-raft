package com.epoint.demo.counter;

import com.open.raft.Status;
import com.open.raft.closure.ReadIndexClosure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * @Description TODO
 * @Date 2022/10/31 17:19
 * @Author jack wu
 */
public class CounterServiceImpl implements CounterService{

    private static final Logger LOG = LoggerFactory.getLogger(CounterServiceImpl.class);

    private final CounterServer counterServer;
    private final Executor readIndexExecutor;

    public CounterServiceImpl(CounterServer counterServer) {
        this.counterServer = counterServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    @Override
    public void get(boolean readOnlySafe, CounterClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }

        this.counterServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    closure.success(getValue());
                    // sendResponse
                    closure.run(Status.OK());
                    return;
                }
                CounterServiceImpl.this.readIndexExecutor.execute(() -> {
                    if (isLeader()) {
                        LOG.debug("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        applyOperation(CounterOperation.createGet(), closure);
                    } else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    @Override
    public void incrementAndGet(long delta, CounterClosure closure) {

    }

    private long getValue() {
        return this.counterServer.getFsm().getValue();
    }
}
