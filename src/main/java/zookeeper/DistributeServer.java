package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 模拟业务服务器 注册到zk集群
 */
public class DistributeServer {
    private static String connectString = "had002:2181,had003:2181,had004:2181";
    private static int sesssionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private String parentNode = "/servers";

    /**
     * 创建zk客户端连接
     * @throws IOException
     */
    public void getConnect() throws IOException {
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
     * 注册服务器到zk
     * @param hostname
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void registerServer(String hostname) throws KeeperException, InterruptedException {
        String create = zkClient.create(parentNode + "/server", hostname.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println(hostname + " is online" + create);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer server = new DistributeServer();
        server.getConnect();

        server.registerServer(args[0]);
        server.business(args[0]);
    }

    /**
     * 业务..
     * @param hostname
     * @throws InterruptedException
     */
    private void business(String hostname) throws InterruptedException {
        System.out.println(hostname + "is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

}
