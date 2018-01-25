package com.zhoutc.zookeeper.api;

/**
 * Description:
 * Author:Zhoutc
 * Date:2018-1-24 14:59
 */
public interface Recoverable {

    boolean recovery();

    String getName();
}
