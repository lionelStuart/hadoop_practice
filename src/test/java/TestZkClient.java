import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestZkClient {

    private static String connectString = "had002:2181,had003:2181,had004:2181";
    private static int sesssionTimeout = 2000;
    private ZooKeeper zkClient = null;

    /**
     * 初始化客户端 设置watch proc
     *
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sesssionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());

                try {
                    zkClient.getChildren("/", true);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * 测试创建节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/idea", "hello idea".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("create: " + nodeCreated);
    }

    /**
     * 测试获取节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);

        System.out.println("-----start---------------------");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("-----End-----------------------");
    }

    /**
     * 测试节点存在
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/idea", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }

}

