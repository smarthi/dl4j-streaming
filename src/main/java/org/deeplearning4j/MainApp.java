package org.deeplearning4j;

import org.apache.camel.main.Main;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * A Camel Application
 */
public class MainApp {

    /**
     * A main() so we can easily run these routing rules in our IDE
     */
    public static void main(String... args) throws Exception {
      /*  Main main = new Main();
        main.addRouteBuilder(new MyRouteBuilder());
        main.run(args);*/
        EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper(2181);
        List<Integer> kafkaPorts = new ArrayList<>();
        // -1 for any available port
        kafkaPorts.add(-1);
        kafkaPorts.add(-1);
        EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        embeddedZookeeper.startup();
        System.out.println("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        System.out.println("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());
        Thread.sleep(10000);
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();


    }

}

