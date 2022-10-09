package com.open.raft.option;

import com.open.raft.util.Copiable;

/**
 * @Description 用于设置跟性能和数据可靠性相关的参数
 * @Date 2022/9/22 18:57
 * @Author jack wu
 */
public class RaftOptions implements Copiable<RaftOptions> {

    /**
     * Maximum number of tasks that can be applied in a batch
     *  (Disruptor handler batch size)
     */
    private int applyBatch = 32;

    @Override
    public RaftOptions copy() {
        return null;
    }

    public int getApplyBatch() {
        return applyBatch;
    }

    public void setApplyBatch(int applyBatch) {
        this.applyBatch = applyBatch;
    }
}
