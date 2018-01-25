package com.zhoutc.zookeeper.client;

import com.zhoutc.zookeeper.api.Recoverable;
import com.zhoutc.zookeeper.watcher.DefaultWatcher;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Description:zookeeper客户端，实现了基本操作
 * Author:Zhoutc
 * Date:2017-12-20 15:21
 */
public class ZkClient {
    private static final Logger logger = LoggerFactory.getLogger(ZkClient.class);
    private static final int WAIT_TIME = 10000;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private ZooKeeper zookeeper;
    /**
     * 客户端连接事件watcher
     */
    private Watcher watcher;
    /**
     * zookeeper连接地址
     */
    private String url;
    /**
     * 客户端连接超时时间
     */
    private int sessionTimeout;
    /**
     * 客户端连接是否正常
     */
    private volatile boolean normal;
    /**
     * 客户端是否正在重试恢复连接
     */
    private volatile boolean retrying;
    /**
     * 是否需要客户端在重新连接后自动恢复watcher
     */
    private volatile boolean autoReWatch;
    /**
     * 连接异常计数器
     */
    private AtomicInteger errorCount = new AtomicInteger(0);
    /**
     * 最大异常数开始重连
     */
    private int RECOVERY_COUNT = 10;
    /**
     * zookeeper data watchers
     */
    private Map<String, Set<Watcher>> dataWatchers = new HashMap<>();
    /**
     * zookeeper exists watchers
     */
    private Map<String, Set<Watcher>> existWatchers = new HashMap<>();
    /**
     * zookeeper child watchers
     */
    private Map<String, Set<Watcher>> childWatchers = new HashMap<>();
    /**
     * zookeeper重连恢复点
     */
    private List<Recoverable> recoverPoints = new ArrayList<>();

    public ZkClient(String url, int sessionTimeout) {
        this.url = url;
        this.sessionTimeout = sessionTimeout;
        watcher = new DefaultWatcher(this);
        connect();
    }

    public ZkClient(String url, int sessionTimeout, List<Recoverable> recoverPoints) {
        if(recoverPoints != null && recoverPoints.size() > 0){
            this.recoverPoints = recoverPoints;
        }
        this.url = url;
        this.sessionTimeout = sessionTimeout;
        watcher = new DefaultWatcher(this);
        connect();
    }

    public ZkClient(String url, int sessionTimeout, List<Recoverable> recoverPoints, boolean autoReWatch) {
        this.autoReWatch = autoReWatch;
        if(recoverPoints != null && recoverPoints.size() > 0){
            this.recoverPoints = recoverPoints;
        }
        this.url = url;
        this.sessionTimeout = sessionTimeout;
        watcher = new DefaultWatcher(this);
        connect();
    }

    public void connect() {
        try {
            if(normal){
                logger.info("zookeeper has recovery!!");
                return;
            }
            zookeeper = new ZooKeeper(url, sessionTimeout, watcher);
            boolean await = connectedSignal.await(WAIT_TIME, TimeUnit.MILLISECONDS);
            normal = zookeeper.getState().isConnected();
            errorCount = new AtomicInteger(0);
            logger.info("connect to zookeeper await={}", await);
        } catch (Exception e) {
            logger.error("connect to zookeeper exception!!", e);
        }
    }

    public synchronized void reconnect(){
        if(normal){
            logger.info("zookeeper has recovery!!");
            return;
        }
        retrying = true;
        connectedSignal = new CountDownLatch(1);
        watcher = new DefaultWatcher(this);
        connect();
        if(normal) {
            logger.info("zookeeper reconnect success!!");
            recover();
            logger.info("zookeeper recover success!!");
            try {
                if(autoReWatch) {
                    rewatch();
                }
                logger.info("zookeeper rewatch success!!");
            }catch (Exception e){
                logger.error("zookeeper rewatch error!!",e);
            }
        }else {
            logger.info("zookeeper reconnect failed!!");
        }
        retrying = false;
    }

    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    /**
     * 创建znode结点
     *
     * @param path 结点路径
     * @param data 结点数据
     * @return true 创建结点成功 false表示结点存在
     * @throws Exception
     */
    public boolean addNodeData(String path, String data, CreateMode mode) {
        try {
            checkServer();
            if (zookeeper.exists(path, false) == null) {
                zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
                return true;
            }
        } catch (Exception e) {
            recovery(e);
            logger.error("addNodeData exception", e);
        }
        return false;
    }


    /**
     * 修改znode
     *
     * @param path 结点路径
     * @param data 结点数据
     * @return 修改结点成功   false表示结点不存在
     */
    public boolean updateNodeData(String path, String data) {
        try {
            checkServer();
            Stat stat;
            if ((stat = zookeeper.exists(path, false)) != null) {
                zookeeper.setData(path, data.getBytes(), stat.getVersion());
                return true;
            }
        } catch (Exception e) {
            recovery(e);
            logger.error("updateNodeData exception", e);
        }
        return false;
    }

    /**
     * 删除结点
     *
     * @param path 结点
     * @return true 删除键结点成功  false表示结点不存在
     */
    public boolean deleteNode(String path) {
        try {
            checkServer();
            Stat stat;
            if ((stat = zookeeper.exists(path, false)) != null) {
                List<String> subPaths = zookeeper.getChildren(path, false);
                if (subPaths.isEmpty()) {
                    zookeeper.delete(path, stat.getVersion());
                    return true;
                } else {
                    for (String subPath : subPaths) {
                        deleteNode(path + "/" + subPath);
                    }
                }
            }
        } catch (Exception e) {
            recovery(e);
            logger.error("deleteNode exception", e);
        }
        return false;
    }


    /**
     * 取到结点数据
     *
     * @param path 结点路径
     * @return null表示结点不存在 否则返回结点数据
     */
    public String getNodeData(String path) {
        String data = null;
        try {
            checkServer();
            Stat stat;
            if ((stat = zookeeper.exists(path, false)) != null) {
                data = new String(zookeeper.getData(path, false, stat));
            } else {
                logger.info("getNodeData path={} 不存在", path);
            }
        } catch (Exception e) {
            recovery(e);
            logger.error("getNodeData exception", e);
        }
        return data;
    }

    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     */
    public boolean exists(String path, boolean watch) {
        try {
            checkServer();
            return zookeeper.exists(path, watch) != null;
        } catch (Exception e) {
            recovery(e);
            logger.error("exists exception!!", e);
        }
        return false;
    }

    /**
     * 判断节点是否存在
     *
     * @param path
     * @return
     */
    public boolean exists(String path, Watcher watcher) {
        try {
            addExistWatcher(path, watcher);
            checkServer();
            return zookeeper.exists(path, watcher) != null;
        } catch (Exception e) {
            recovery(e);
            logger.error("exists exception!!", e);
        }
        return false;
    }

    /**
     * 支持多层节点创建
     *
     * @param path
     * @param mode
     */
    public boolean multiLevelCreate(String path, String data, CreateMode mode) {
        try {
            checkServer();
            if (path != null && !("").equals(path.trim())) {
                if (exists(path, false)) {
                    return false;
                }
                String[] paths = path.trim().split("/");
                StringBuilder levelPath = new StringBuilder("");
                for (int i = 0; i < paths.length; i++) {
                    String catalog = paths[i];
                    if (catalog != null && !("").equals(catalog)) {
                        levelPath.append("/").append(catalog);
                        if (!exists(levelPath.toString(), false)) {
                            if (i < paths.length - 1) {
                                zookeeper.create(levelPath.toString(), new byte[1], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            } else {
                                zookeeper.create(levelPath.toString(), data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
                            }
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            recovery(e);
            logger.error("multiLevelCreate exception!!", e);
        }
        return false;
    }

    /**
     * 获取目录子节点
     *
     * @param path
     */
    public List<String> getChildren(String path) {
        try {
            checkServer();
            return zookeeper.getChildren(path, false);
        } catch (Exception e) {
            recovery(e);
            logger.error("getChildren exception!!", e);
        }
        return new ArrayList<>();
    }

    /**
     * 获取目录子节点
     *
     * @param path
     */
    public List<String> getChildren(String path, Watcher watcher) {
        try {
            checkServer();
            addChildWatcher(path, watcher);
            return zookeeper.getChildren(path, watcher);
        } catch (Exception e) {
            recovery(e);
            logger.error("getChildren exception!!", e);
        }
        return null;
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (zookeeper != null) {
                zookeeper.close();
                logger.info("release connection success !!!");
            }
        } catch (Exception e) {
            logger.error("release connection error ," + e.getMessage(), e);
        }
    }

    private boolean checkServer(){
        if(!normal) {
            if(retrying){
                logger.info("zookeeper正在尝试恢复!!");
                return false;
            }
            reconnect();
        }
        return normal;
    }

    private void recovery(Exception e) {
        if (e instanceof KeeperException.SessionExpiredException) {
            int count = errorCount.incrementAndGet();
            logger.info("SessionExpiredException 异常次数:{} ," + count);
            if (count > RECOVERY_COUNT) {
                normal = false;
            }
        }
        if (e instanceof KeeperException.ConnectionLossException) {
            int count = errorCount.incrementAndGet();
            logger.info("ConnectionLossException 异常次数:{} ," + count);
            if (count > RECOVERY_COUNT) {
                normal = false;
            }
        }
    }

    public synchronized void addExistWatcher(String path, Watcher watcher){
        Set<Watcher> watcherSet = existWatchers.get(path);
        if(watcherSet == null){
            watcherSet = new HashSet<>();
            watcherSet.add(watcher);
            existWatchers.put(path,watcherSet);
        }else {
            watcherSet.add(watcher);
        }
    }

    public synchronized void addChildWatcher(String path, Watcher watcher){
        Set<Watcher> watcherSet = childWatchers.get(path);
        if(watcherSet == null){
            watcherSet = new HashSet<>();
            watcherSet.add(watcher);
            childWatchers.put(path,watcherSet);
        }else {
            watcherSet.add(watcher);
        }
    }

    public void rewatch() {
        if (childWatchers != null) {
            for (Map.Entry entry : childWatchers.entrySet()) {
                Set<Watcher> watcherSet = (Set<Watcher>) entry.getValue();
                String path = (String) entry.getKey();
                if(watcherSet == null || path == null){
                    continue;
                }
                for (Watcher watcher : watcherSet) {
                    getChildren(path, watcher);
                    logger.info("zookeeper rewatch child path={},watcher={}", path, watcher);
                }
            }
        }

        if (existWatchers != null) {
            for (Map.Entry entry : existWatchers.entrySet()) {
                Set<Watcher> watcherSet = (Set<Watcher>) entry.getValue();
                String path = (String) entry.getKey();
                if(watcherSet == null || path == null){
                    continue;
                }
                for (Watcher watcher : watcherSet) {
                    exists(path, watcher);
                    logger.info("zookeeper rewatch exists path={},watcher={}", path, watcher);
                }
            }
        }
    }

    private void recover() {
        if(recoverPoints == null){
            logger.info("没有恢复点进行恢复");
            return;
        }
        for (Recoverable recoverable : recoverPoints) {
            boolean result = false;
            try {
                result = recoverable.recovery();
            } catch (Exception e) {
                logger.error("recover task={} exception", recoverable.getName(), e);
            }
            logger.info("recover task={} 恢复结果:{} ", recoverable.getName(), result);
        }
    }

    public CountDownLatch getConnectedSignal() {
        return connectedSignal;
    }

    public boolean isNormal() {
        return normal;
    }

    public void setNormal(boolean normal) {
        this.normal = normal;
    }

    public boolean isRetrying() {
        return retrying;
    }

    public void setRetrying(boolean retrying) {
        this.retrying = retrying;
    }
}
