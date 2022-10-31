package com.epoint.demo.counter;

import com.open.raft.INode;

/**
 * @Description TODO
 * @Date 2022/10/31 17:26
 * @Author jack wu
 */
public class CountServer {
    private RaftGroupService    raftGroupService;
    private INode node;
    private CounterStateMachine fsm;
}
