package com.epoint.demo.counter;

import com.open.raft.Closure;
import com.open.raft.Iterator;
import com.open.raft.Status;
import com.open.raft.core.StateMachineAdapter;
import com.open.raft.error.RaftError;
import com.open.raft.storage.snapshot.SnapshotWriter;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Description TODO
 * @Date 2022/11/16 17:36
 * @Author jack wu
 */
public class CounterStateMachine extends StateMachineAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(CounterStateMachine.class);

    /**
     * Counter value
     */
    private final AtomicLong value = new AtomicLong(0);
    /**
     * Leader term
     */
    private final AtomicLong leaderTerm = new AtomicLong(-1);

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onApply(Iterator iter) {

    }

    /**
     * 这个方法会将数据获取之后写到文件内，然后在保存快照文件后调用传入的参数 closure.run(status) 通知调用者保存成功或者失败。
     * @param writer
     * @param done
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        final long currVal = this.value.get();
        Utils.runInThread(() -> {
            final CounterSnapshotFile snapshot = new CounterSnapshotFile(writer.getPath() + File.separator + "data");
            if (snapshot.save(currVal)) {
                if (writer.addFile("data")) {
                    done.run(Status.OK());
                } else {
                    done.run(new Status(RaftError.EIO, "Fail to add file to writer"));
                }
            } else {
                done.run(new Status(RaftError.EIO, "Fail to save counter snapshot %s", snapshot.getPath()));
            }
        });
    }
}
