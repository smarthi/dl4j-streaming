package org.deeplearning4j;

import kafka.serializer.StringDecoder;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * A Camel Application
 */
public class MainApp {

    /**
     * A main() so we can easily run these routing rules in our IDE
     */
    public static void main(String... args) throws Exception {
      /*  Main main = new Main();
        main.addRouteBuilder(new CamelKafkaRouteBuilder());
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

        CamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new CamelKafkaRouteBuilder("test",embeddedKafkaCluster.getPorts().get(0)));
        camelContext.start();




        // Create context with a 2 seconds batch interval
   /*     SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        Set<String> topicsSet = new HashSet<>(Arrays.asList("test"));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", embeddedKafkaCluster.getBrokerList());

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                System.out.println(tuple2._2());
                return tuple2._2();
            }
        });

        lines.print();
        // Start the computation
        jssc.start();
        jssc.awaitTermination();
*/

        Thread.sleep(10000);
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();
        camelContext.stop();
        //jssc.stop();


    }

}

