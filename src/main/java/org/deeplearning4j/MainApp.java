package org.deeplearning4j;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.codec.binary.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.canova.api.writable.Writable;
import org.deeplearning4j.streaming.serde.RecordDeSerializer;
import org.deeplearning4j.streaming.serde.RecordSerializer;

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
  /*      EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper(2181);
        List<Integer> kafkaPorts = new ArrayList<>();
        // -1 for any available port
        kafkaPorts.add(-1);
        EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        embeddedZookeeper.startup();
        System.out.println("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        System.out.println("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());
*/

        //"file:src/test/resources/?fileName=iris.dat&noop=true"
        int port = 9092;
        final String topicName = "test3";
        CamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new CamelKafkaRouteBuilder.Builder().
                camelContext(camelContext)
                .inputFormat("org.canova.api.formats.input.impl.ListStringInputFormat").
                        topicName(topicName).camelContext(camelContext)
                .dataTypeUnMarshal("csv")
                .inputUri("file:src/test/resources/?fileName=iris.dat&noop=true").
                        kafkaBrokerList("localhost:9092").processor( new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        Collection<Collection<Writable>> record = (Collection<Collection<Writable>>) exchange.getIn().getBody();
                        exchange.getIn().setHeader(KafkaConstants.KEY, UUID.randomUUID().toString());
                        exchange.getIn().setHeader(KafkaConstants.PARTITION_KEY, new Random().nextInt(1));
                        byte[] bytes = new RecordSerializer().serialize(topicName,record);
                        String base64 = org.apache.commons.codec.binary.Base64.encodeBase64String(bytes);
                        exchange.getIn().setBody(base64,String.class);
                    }
                }).build());
        camelContext.start();


        // Create context with a 2 seconds batch interval
        // SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[*]");
        //  JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        String brokerList = "localhost:" + port;
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("group.id", "canova");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_DEFAULT_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_DEFAULT_DESERIALIZER);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        System.setProperty("hadoop.home.dir", "/home/agibsonccc/hadoop");


        SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list","localhost:9092");

        JavaPairInputDStream<String,String> messages = KafkaUtils.createStream(
                jssc,
                "localhost:2181",
                "canova",
                Collections.singletonMap(topicName,3));
        messages.foreach(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
                Map<String,String> map = stringStringJavaPairRDD.collectAsMap();
                if(!map.isEmpty()) {
                    Iterator<String> vals = map.values().iterator();
                    while(vals.hasNext()) {
                        try {
                            byte[] bytes = org.apache.commons.codec.binary.Base64.decodeBase64(vals.next());
                            Collection<Collection<Writable>> records = new RecordDeSerializer().deserialize("topic",bytes);
                            System.out.println(records);
                        }catch (Exception e) {
                            System.out.println("Error serializing");
                        }
                    }

                }
                return null;
            }
        });

        // Start the computation
        jssc.start();
        Thread.sleep(30000);
        jssc.awaitTermination();

        camelContext.stop();


    }
}


