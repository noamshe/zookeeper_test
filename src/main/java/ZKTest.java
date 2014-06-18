
public class ZKTest {

    private static final String hostPort = "127.0.0.1:2181";

    public static void main(String args[]) {
        ZKTest test = new ZKTest();
        test.start();
        System.out.println("Done test");
    }


    public void start() {
        String node = "/test";
        try {
            ZKClient client1 = new ZKClient("Client-1", hostPort, node);
            ZKClient client2 = new ZKClient("Client-2", hostPort, node);
            ZKClient client3 = new ZKClient("Client-3", hostPort, node);
            client1.awaitConnected();
            client2.awaitConnected();
            client3.awaitConnected();

            client1.createNode();
            client2.createNode();
            client3.createNode();

            client1.registerForNodeChanges();
            client2.registerForNodeChanges();
            client3.registerForNodeChanges();

            for (int i = 0; i < 10; i++) {
                client1.updateNodeValue(i);
                Thread.sleep(100000);
            }

        } catch (Exception e) {
            System.out.println("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }


}
