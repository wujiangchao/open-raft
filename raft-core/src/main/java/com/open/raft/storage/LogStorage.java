package com.open.raft.storage;

import com.open.raft.Lifecycle;
import com.open.raft.option.LogStorageOptions;

/**
 * @Description LogStorage 是日志存储实现，默认实现基于 RocksDB 存储，
 *              通过 LogStorage 接口扩展自定义日志存储实现；
 * @Date 2022/9/27 7:17
 * @Author jack wu
 */
public interface LogStorage extends Lifecycle<LogStorageOptions> {

}
