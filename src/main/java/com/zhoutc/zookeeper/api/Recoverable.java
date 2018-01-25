package com.zhoutc.zookeeper.api;

/**
 * Description: 业务恢复接口，业务通过实现该接口，在zookeeper重新连接时恢复业务初始化数据
 * Author:Zhoutc
 * Date:2018-1-24 14:59
 */
public interface Recoverable {

    boolean recovery();

    String getName();
}
