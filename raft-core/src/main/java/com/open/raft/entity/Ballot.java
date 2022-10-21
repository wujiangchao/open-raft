
package com.open.raft.entity;


import com.open.raft.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * A ballot to vote.
 *
 */
public class Ballot {

    public static final class PosHint {
        int pos0 = -1; // position in current peers
        int pos1 = -1; // position in old peers
    }

    public static class UnfoundPeerId {
        PeerId peerId;
        boolean found;
        int index;

        public UnfoundPeerId(PeerId peerId, int index, boolean found) {
            super();
            this.peerId = peerId;
            this.index = index;
            this.found = found;
        }
    }

    private final List<UnfoundPeerId> peers = new ArrayList<>();
    private int quorum;
    private final List<UnfoundPeerId> oldPeers = new ArrayList<>();
    private int oldQuorum;

    /**
     * Init the ballot with current conf and old conf.
     * 初始化预投票箱是调用了Ballot的init方法进行初始化，分别传入新的集群节点信息，和老的集群节点信息
     *
     * @param conf    current configuration
     * @param oldConf old configuration
     * @return true if init success
     */
    public boolean init(final Configuration conf, final Configuration oldConf) {
        this.peers.clear();
        this.oldPeers.clear();
        this.quorum = this.oldQuorum = 0;
        int index = 0;
        //初始化新的节点
        if (conf != null) {
            for (final PeerId peer : conf) {
                this.peers.add(new UnfoundPeerId(peer, index++, false));
            }
        }

        //设置需要多少票数才能成为leader
        //这个属性会在每获得一个投票就减1，当减到0以下时说明获得了足够多的票数，就代表预投票成功。
        this.quorum = this.peers.size() / 2 + 1;
        if (oldConf == null) {
            return true;
        }
        index = 0;
        for (final PeerId peer : oldConf) {
            this.oldPeers.add(new UnfoundPeerId(peer, index++, false));
        }

        this.oldQuorum = this.oldPeers.size() / 2 + 1;
        return true;
    }

    private UnfoundPeerId findPeer(final PeerId peerId, final List<UnfoundPeerId> peers, final int posHint) {
        if (posHint < 0 || posHint >= peers.size() || !peers.get(posHint).peerId.equals(peerId)) {
            for (final UnfoundPeerId ufp : peers) {
                if (ufp.peerId.equals(peerId)) {
                    return ufp;
                }
            }
            return null;
        }

        return peers.get(posHint);
    }

    public PosHint grant(final PeerId peerId, final PosHint hint) {
        //grant方法会根据peerId去集群集合里面去找被封装的UnfoundPeerId实例，然后判断一下，如果没有被记录过，
        // 那么就将quorum减一，表示收到一票，然后将found设置为ture表示已经找过了
        UnfoundPeerId peer = findPeer(peerId, this.peers, hint.pos0);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                this.quorum--;
            }
            hint.pos0 = peer.index;
        } else {
            hint.pos0 = -1;
        }
        if (this.oldPeers.isEmpty()) {
            hint.pos1 = -1;
            return hint;
        }
        peer = findPeer(peerId, this.oldPeers, hint.pos1);
        if (peer != null) {
            if (!peer.found) {
                peer.found = true;
                this.oldQuorum--;
            }
            hint.pos1 = peer.index;
        } else {
            hint.pos1 = -1;
        }

        return hint;
    }

    public void grant(final PeerId peerId) {
        grant(peerId, new PosHint());
    }

    /**
     * Returns true when the ballot is granted.
     *
     * @return true if the ballot is granted
     */
    public boolean isGranted() {
        return this.quorum <= 0 && this.oldQuorum <= 0;
    }
}
