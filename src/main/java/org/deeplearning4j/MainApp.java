package org.deeplearning4j;

import kafka.serializer.StringDecoder;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.canova.api.writable.Writable;
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
        final String topicName = "test2";
        CamelContext camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new CamelKafkaRouteBuilder.Builder().
                camelContext(camelContext)
                .inputFormat("org.canova.api.formats.input.impl.ListStringInputFormat").
                        topicName("test").camelContext(camelContext)
                .dataTypeUnMarshal("csv")
                .inputUri("file:src/test/resources/?fileName=iris.dat&noop=true").
                        kafkaBrokerList("localhost:9092").processor( new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        Collection<Collection<Writable>> record = (Collection<Collection<Writable>>) exchange.getIn().getBody();
                        exchange.getIn().setHeader(KafkaConstants.KEY, UUID.randomUUID().toString());
                        exchange.getIn().setHeader(KafkaConstants.PARTITION_KEY, 0);
                        exchange.getIn().setBody(new String(new RecordSerializer().serialize(topicName,record)),String.class);
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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
                //System.out.println("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",record.topic(), record.partition(), record.offset(), record.key(), record.value());

            }

            if (!records.isEmpty())
                break;
        }

        consumer.commitSync();
        camelContext.stop();


    }
}


