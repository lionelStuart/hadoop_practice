package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 模拟客户端，监听服务器状态
 */
public class DistributeClient {
    private static String connectString = "had002:2181,had003:2181,had004:2181";
    private static int sesssionTimeout = 2000;
    private ZooKeeper zkClient = null;
    private String parentNode = "/servers";
    private volatile ArrayList<String> serverList = new ArrayList<String>();

    /**
     * 创建zk客户端连接
     *
     * @throws IOException
     */
    public void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sesssionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());

                try {
                    getServerList();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void getServerList() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(parentNode, true);
        ArrayList<String> servers = new ArrayList();

        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        serverList = servers;

        System.out.println(serverList);
    }

    public void business() throws InterruptedException {
        System.out.println("client is working ...");

        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {

        DistributeClient client = new DistributeClient();
        client.getConnect();

        client.getServerList();

        client.business();

    }

}