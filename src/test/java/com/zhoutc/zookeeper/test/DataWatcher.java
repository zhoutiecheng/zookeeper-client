package com.zhoutc.zookeeper.test;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Description:
 * Author:Zhoutc
 * Date:2017-12-21 11:07
 */
public class DataWatcher implements Watcher{

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event.toString());

    }
}
