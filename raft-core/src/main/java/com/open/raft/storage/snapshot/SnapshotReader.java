/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.open.raft.storage.snapshot;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.RaftOutter.SnapshotMeta;
import com.open.raft.Lifecycle;

import java.io.Closeable;

/**
 * Snapshot reader.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 4:53:40 PM
 */
public abstract class SnapshotReader extends Snapshot implements Closeable, Lifecycle<Void> {

    /**
     * Load the snapshot metadata.
     */
    public abstract SnapshotMeta load();

    /**
     * Generate uri for other peers to copy this snapshot.
     * Return an empty string if some error has occur.
     */
    public abstract String generateURIForCopy();
}
