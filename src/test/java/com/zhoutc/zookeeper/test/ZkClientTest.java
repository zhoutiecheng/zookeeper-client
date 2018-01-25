package com.zhoutc.zookeeper.test;

import com.zhoutc.zookeeper.client.ZkClient;
import mockit.integration.junit4.JMockit;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * Description:
 * Author:Zhoutc
 * Date:2017-12-20 15:44
 */
@RunWith(JMockit.class)
public class ZkClientTest {
    String url = "192.168.100.222:2181";
    int timeOut = 3000;
    @Test
    public void testZkClient() throws Exception{
        ZkClient zkClient = new ZkClient(url, timeOut,null);
        boolean result = zkClient.addNodeData("/test122", "123" ,CreateMode.PERSISTENT);
        System.out.println(result);

    }

    @Test
    public void testZkClient0() throws Exception{
        ZkClient zkClient = new ZkClient(url, timeOut,null);
        zkClient.multiLevelCreate("/test/1/1/1/", "112233" ,CreateMode.PERSISTENT);


    }

    @Test
    public void testZkClient1() throws Exception{
        ZkClient zkClient = new ZkClient(url, timeOut,null);
        boolean result = zkClient.addNodeData("/test/1/1/2", "123" ,CreateMode.EPHEMERAL);
        System.out.println(result);
        Thread.sleep(10000);
        System.out.println("over!!");

    }

    @Test
    public void testZkClient2() throws Exception{
        ZkClient zkClient = new ZkClient(url, timeOut,null);
        for(int i  = 0 ; i < 5 ;i++){
            boolean result = zkClient.addNodeData("/test/1/2/"+i, "123" ,CreateMode.PERSISTENT);
            System.out.println(result);
        }
        System.out.println("over!!");

    }

    @Test
    public void testZkClient3() throws Exception{
        ZkClient zkClient = new ZkClient(url, timeOut,null);
        List<String> list = zkClient.getChildren("/test/1",new DataWatcher());
        System.out.println(list.toString());
        System.out.println("sleep!!");
        Thread.sleep(2000);
        boolean create = zkClient.multiLevelCreate("/test/1/7/3/2","",CreateMode.PERSISTENT);
        System.out.println("create!!"+create);

        Thread.sleep(1000000);
        System.out.println("over!!");

    }

    @Test
    public void testZkClient4() throws Exception{
        ZkClient zkClient = new ZkClient(url, timeOut,null);
        DataWatcher dataWatcher = new DataWatcher();
        for (int x = 0 ; x < 3;x++){
            List<String> list = zkClient.getChildren("/test/1/1",dataWatcher);
            System.out.println(list.toString());
        }

        List<String> list = zkClient.getChildren("/test/1/1/0",new DataWatcher());
        System.out.println("ass:"+list);



    }

    @Test
    public void testZkClient5() throws Exception{
        ZkClient zkClient = new ZkClient("10.37.18.106:2181", timeOut,null);
    }


}
