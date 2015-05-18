package test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Collection;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class Node {
    private static Logger logger = Logger.getLogger(Node.class);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setDaemon(true);
            return thread;        }
    });

    private final HazelcastInstance hazelcast;

    private IMap<String, String> map;
    private boolean valueSet;
    private String clientListenerKey;

    public Node()
    {
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        config.setProperty("hazelcast.logging.type", "log4j");

        hazelcast = Hazelcast.newHazelcastInstance(config);
    }

    public void start()
    {
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //changeValue();
                //dump();
            }
        }, 2, 2, TimeUnit.SECONDS);

        clientListenerKey = hazelcast.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                System.out.println("Client connected: "+client.getSocketAddress());
                logger.info("Client connected: "+client.getSocketAddress());
            }

            @Override
            public void clientDisconnected(Client client) {
                System.out.println("Client disconnected: "+client.getSocketAddress());
                logger.info("Client disconnected: "+client.getSocketAddress());
            }
        });
        map = hazelcast.getMap("TestMap");
        try {
            Set<Map.Entry<String, String>> entries = map.entrySet();
            System.out.println("TestMap contains "+entries.size()+" entries");
            logger.info("TestMap contains "+entries.size()+" entries");
        }
        catch (Exception e)
        {
            logger.error("Exception occurred while trying to get entrySet", e);
        }

        map.set(hazelcast.getCluster().getLocalMember().getSocketAddress().toString(), hazelcast.getCluster().getLocalMember().getSocketAddress().getHostName());
    }

    private void changeValue() {
        if(valueSet) {
            map.remove(hazelcast.getCluster().getLocalMember().getUuid());
        }
        else {
            map.set(hazelcast.getCluster().getLocalMember().getUuid(), hazelcast.getCluster().getLocalMember().getSocketAddress().getHostName());
        }
        valueSet = !valueSet;
    }

    public void stop()
    {
        hazelcast.getClientService().removeClientListener(clientListenerKey);
        hazelcast.getLifecycleService().shutdown();
    }

    private void dump() {
        Collection<Client> clients = hazelcast.getClientService().getConnectedClients();
        System.out.println("Clients connected: "+clients.size());
        for(Client client : clients)
        {
            System.out.println("- Type: "+client.getClientType() + ", address: "+client.getSocketAddress());
        }

        //System.out.println("Number of entries in map: "+map.size());
        //logger.info("Number of entries in map: "+map.size());
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        
        Node node = new Node();
        node.start();

        System.out.println("Node started");

        System.out.println("Press ENTER to stop node");
        Scanner sc = new Scanner(System.in);
        sc.nextLine();
        System.out.println("Stopping node");

        node.stop();
    }
}
