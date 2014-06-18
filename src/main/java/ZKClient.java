
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

public class ZKClient implements Watcher {

    private ZooKeeper zk;
    private String clientName;

    private CountDownLatch connectedLatch = new CountDownLatch(1);

    private final static Object lock = new Object();
    private static boolean nodeCreated = false;

    private String node;
    private byte[] nodeData;
    private int nodeVersion;

    public ZKClient(String clientName, String hostPort, String node) throws IOException {
        this.clientName = clientName;
        this.node = node;
        zk = new ZooKeeper(hostPort, 300000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            log(event.toString());
            if (event.getState() == Event.KeeperState.SyncConnected) {
                if (event.getType() == Event.EventType.None) {
                    connectedLatch.countDown();
                } else if (event.getType() == Event.EventType.NodeDataChanged) {
                    nodeData = zk.getData(node, this, null);
                    setNodeVersion(zk.exists(node, true).getVersion());
                    //log("ZNode updated: " + bytesToInt(nodeData));
                    log("ZNode updated: " + new String(nodeData));
                } else {
                    log("Unknown event: " + event.toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void awaitConnected() throws InterruptedException {
        connectedLatch.await();
    }

    public void createNode() throws KeeperException, InterruptedException {
        synchronized (lock) {
            if (!nodeCreated) {
                nodeCreated = true;
                Stat s = zk.exists(node, false);
                nodeData = intToBytes(0);
                if (s == null) {
                    zk.create(node, nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    setNodeVersion(zk.exists(node, false).getVersion());
                } else {
                    setNodeVersion(s.getVersion());
                    zk.setData(node, nodeData, nodeVersion);
                }
                log("Created node");
            }
        }
    }

    public void registerForNodeChanges() throws KeeperException, InterruptedException {
        zk.exists(node, true);
    }

    public void updateNodeValue(int newValue) throws KeeperException, InterruptedException {
        log("Updating node to value " + newValue + ", node version: " + nodeVersion);
        setNodeVersion(zk.exists(node, false).getVersion());
//        zk.setData(node, intToBytes(newValue), nodeVersion);
        zk.setData(node, ("TEST_" + newValue).getBytes(), nodeVersion);
    }

    private void log(String line) {
        System.out.println(clientName + " -> " + line);
    }

    private static byte[] intToBytes(int intValue) {
        return ByteBuffer.allocate(4).putInt(intValue).array();
    }

    private int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    private void setNodeVersion(int nodeVersion) {
        //log("setting node version: " + nodeVersion);
        this.nodeVersion = nodeVersion;
    }

}
