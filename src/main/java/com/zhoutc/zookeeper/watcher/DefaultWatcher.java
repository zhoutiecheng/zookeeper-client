package com.zhoutc.zookeeper.watcher;


import com.zhoutc.zookeeper.client.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:
 * Author:Zhoutc
 * Date:2017-12-20 15:34
 */
public class DefaultWatcher implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(DefaultWatcher.class);
    private ZkClient zkClient;
    public DefaultWatcher(ZkClient zkClient){
        this.zkClient = zkClient;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        final Event.KeeperState state = watchedEvent.getState();
        if(watchedEvent.getType() != Event.EventType.None){
            return;
        }
        switch (state) {
            case SyncConnected:
                logger.info("zookeeper SyncConnected!!");
                zkClient.getConnectedSignal().countDown();
                break;
            case Disconnected:
                logger.warn("zookeeper Disconnected!!");
                zkClient.setNormal(false);
                zkClient.close();
                break;
            case Expired:
                logger.error("zookeeper Expired!!");
                reconnect();
                break;
            case AuthFailed:
                logger.error("zookeeper AuthFailed!!");
                break;
            default:
                break;
        }
    }

    private void reconnect() {
        try {
            if (zkClient.isRetrying()) {
                logger.info("zookeeper正在尝试恢复!!");
                return;
            }
            zkClient.reconnect();
        } catch (Exception e) {
            logger.error("connect exception", e);
        }
    }
}
