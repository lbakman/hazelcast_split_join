package test;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

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

    private static String mapName = "TestMap";
    private IMap<String, String> map;
    private boolean valueSet;
    private String migrationListenerKey;

    public Node()
    {
        final TestMapLoader loader = new TestMapLoader();
        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        config.setProperty("hazelcast.logging.type", "log4j");
        MapConfig mapConfig = config.getMapConfig(mapName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(loader);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        hazelcast = Hazelcast.newHazelcastInstance(config);
    }

    public void start()
    {
/*
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                //changeValue();
                //dump();
            }
        }, 2, 2, TimeUnit.SECONDS);
*/
        migrationListenerKey = hazelcast.getPartitionService().addMigrationListener(new MigrationListener() {
            @Override
            public void migrationStarted(MigrationEvent migrationEvent) {
                logger.info("Migration started: " + migrationEvent.toString());
            }

            @Override
            public void migrationCompleted(MigrationEvent migrationEvent) {
                logger.info("Migration completed: "+migrationEvent.toString());
            }

            @Override
            public void migrationFailed(MigrationEvent migrationEvent) {
                logger.warn("Migration failed: " + migrationEvent.toString());
            }
        });

        map = hazelcast.getMap(mapName);
        //fillMap();
        dump();
    }

    public void stop()
    {
        hazelcast.getPartitionService().removeMigrationListener(migrationListenerKey);
        hazelcast.getLifecycleService().shutdown();
    }

    private void dump() {
        try {
            Set<Map.Entry<String, String>> entries = map.entrySet();
            System.out.println("TestMap contains "+entries.size()+" entries");
            logger.info("TestMap contains "+entries.size()+" entries");
        }
        catch (Exception e)
        {
            logger.error("Exception occurred while trying to get entrySet", e);
        }
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
