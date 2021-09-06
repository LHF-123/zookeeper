package com.atguigu.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author LHF
 * @date 2021/8/25 21:47
 */
public class ZkClient {

    private ZooKeeper zkCli;
    private static final String CONNECT_STRING = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static final int SESSION_TIMEOUT = 2000;

    @Before
    public void before() throws IOException {

        //e -> {} lambda表达式当形参列表非空参，并且只有一个形参，并且类型也省略了，那么
            //此时()也可以省略，如果类型没有省略，那么()也不能省略
        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, e -> {
            System.out.println("默认回调函数");
        });

    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", e -> {
            System.out.println("自定义回调函数");
        });

        System.out.println("====================================================");
        for (String child :
                children) {
            System.out.println(child);
        }
        System.out.println("====================================================");

        //因为是异步通信，要有个线程等着。顺便测试下是否在监视根目录
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        // ZooDefs.Ids.OPEN_ACL_UNSAFE谁都可以访问不安全的
        //CreateMode创建的节点的类型，是否有序，是否为临时
        String s = zkCli.create("/Idea", "Idea2018".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(s);

        //阻塞线程不然看不到临时节点
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        byte[] data = zkCli.getData("/lhf0000000007", true, new Stat());

        String str = new String(data);
        System.out.println(str);
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        //先看节点是否存在
        Stat exist = zkCli.exists("/lhf0000000007",false);
        //exist.getVersion()得到节点的版本号
        Stat stat = zkCli.setData("/lhf0000000007", "123".getBytes(), exist.getVersion());

        System.out.println(stat.getDataLength());
    }

    @Test
    public void stat() throws KeeperException, InterruptedException {
        Stat exist = zkCli.exists("/Idea",false);
        if(exist == null){
            System.out.println("节点不存在");
        }else {
            System.out.println(exist.getDataLength());
        }
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/abc",false);
        if (exists != null)
            zkCli.delete("/abc", exists.getVersion());
    }

    public void register() throws KeeperException, InterruptedException {
        //得到/a节点的值，并监听该节点的变化，如果节点有改动，调用watch再次执行该方法
        byte[] data = zkCli.getData("/a", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    register();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);
        System.out.println(new String(data));
    }

    @Test
    public void testRegister(){
        try {
            register();
            //阻塞该方法以便监听/a节点的变化
            Thread.sleep(Long.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
