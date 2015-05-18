package test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.*;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Scanner;

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);

    private final HazelcastInstance hazelcast;
    private String lifecycleListenerId;
    private IMap<String, String> map;

    public Client()
    {
        XmlClientConfigBuilder configBuilder = new XmlClientConfigBuilder();
        ClientConfig config = configBuilder.build();

        hazelcast = HazelcastClient.newHazelcastClient(config);
    }

    public void start()
    {
        //membershipListenerId = hazelcast.getCluster().addMembershipListener(membershipListener);
        lifecycleListenerId = hazelcast.getLifecycleService().addLifecycleListener(lifecycleListener);
        map = hazelcast.getMap("TestMap");
        map.addEntryListener(new EntryListener<String, String>() {
            @Override
            public void entryAdded(EntryEvent<String, String> event) {
                logger.info("Entry added: "+event.getValue());
            }

            @Override
            public void entryRemoved(EntryEvent<String, String> event) {
                logger.info("Entry removed: "+event.getValue());
            }

            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                logger.info("Entry updated: "+event.getValue());
            }

            @Override
            public void entryEvicted(EntryEvent<String, String> event) {
                logger.info("Entry evicted: "+event.getValue());
            }

            @Override
            public void mapEvicted(MapEvent event) {
                logger.info("Map evicted: "+event.getName());
            }

            @Override
            public void mapCleared(MapEvent event) {
                logger.info("Map cleared: "+event.getName());
            }
        }, true);
    }
/*
    private final MembershipListener membershipListener = new MembershipListener() {
        @Override
        public void memberAdded(MembershipEvent event) {
            int memberCount = event.getMembers().size();
            System.out.println("Member added: " + memberCount + "(" + memberCount + ") => " + event.getMember().toString());
            logger.debug("Member added: " + memberCount + "(" + memberCount + ") => " + event.getMember().toString());
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            int memberCount = event.getMembers().size();
            System.out.println("Member removed: " + memberCount + "(" + memberCount + ") => " + event.getMember().toString());
            logger.debug("Member removed: " + memberCount + "(" + memberCount + ") => " + event.getMember().toString());
        }

        @Override
        public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {

        }
    };*/

    private final LifecycleListener lifecycleListener = new LifecycleListener() {
        @Override
        public void stateChanged(LifecycleEvent event) {
            switch (event.getState())
            {
                case CLIENT_CONNECTED:
                    System.out.println("Lifecycle " + event.getState().name());
                    logger.debug("Lifecycle " + event.getState().name());
                    break;

                case CLIENT_DISCONNECTED:
                    System.out.println("Lifecycle " + event.getState().name());
                    logger.debug("Lifecycle " + event.getState().name());
                    break;

                default:
                    break;
            }
        }
    };

    public void stop()
    {
        hazelcast.getLifecycleService().removeLifecycleListener(lifecycleListenerId);
        //hazelcast.getCluster().removeMembershipListener(membershipListenerId);
        hazelcast.getLifecycleService().shutdown();
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");
        System.setProperty("hazelcast.logging.type", "log4j");

        Client client = new Client();
        client.start();

        System.out.println("Client started");

        System.out.println("Press ENTER to stop client");
        Scanner sc = new Scanner(System.in);
        sc.nextLine();
        System.out.println("Stopping client");

        client.stop();
    }
}
