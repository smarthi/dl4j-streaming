package org.deeplearning4j.streaming.pipeline;

import lombok.Builder;
import lombok.Data;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.canova.api.writable.Writable;
import org.deeplearning4j.streaming.routes.CamelKafkaRouteBuilder;
import org.deeplearning4j.streaming.serde.RecordDeSerializer;
import org.deeplearning4j.streaming.serde.RecordSerializer;

import java.util.*;

/**
 * Spark streaming pipeline.
 *
 */
@Data
@Builder
public class SparkStreamingPipeline {
    private String kafkaTopic;
    private String inputUri;
    private String inputFormat;
    private String kafkaBroker;
    private String zkHost;
    private CamelContext camelContext;
    private String sparkMaster;
    private String hadoopHome;
    private String dataType;
    private String sparkAppName = "canova";
    private Duration streamingDuration =  Durations.seconds(1);
    private int kafkaPartitions = 1;
    private JavaStreamingContext jssc;
    private SparkConf sparkConf;
    private Function<JavaPairRDD<String, String>, Void> streamProcessor;

    /**
     * Initialize the pipeline
     * setting up camel routes,
     * kafka,canova,and the
     * spark streaming DAG.
     * @throws Exception
     */
    public void init() throws Exception {
        if (camelContext == null)
            camelContext = new DefaultCamelContext();

        camelContext.addRoutes(new CamelKafkaRouteBuilder.Builder().
                camelContext(camelContext)
                .inputFormat(inputFormat).
                        topicName(kafkaTopic).camelContext(camelContext)
                .dataTypeUnMarshal(dataType)
                .inputUri(inputUri).
                        kafkaBrokerList(kafkaBroker).processor(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        Collection<Collection<Writable>> record = (Collection<Collection<Writable>>) exchange.getIn().getBody();
                        exchange.getIn().setHeader(KafkaConstants.KEY, UUID.randomUUID().toString());
                        exchange.getIn().setHeader(KafkaConstants.PARTITION_KEY, new Random().nextInt(kafkaPartitions));
                        byte[] bytes = new RecordSerializer().serialize(kafkaTopic, record);
                        String base64 = org.apache.commons.codec.binary.Base64.encodeBase64String(bytes);
                        exchange.getIn().setBody(base64, String.class);
                    }
                }).build());


        if(hadoopHome == null)
            hadoopHome = System.getProperty("java.io.tmpdir");
        System.setProperty("hadoop.home.dir", hadoopHome);
        sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster);
        jssc = new JavaStreamingContext(sparkConf, streamingDuration);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaBroker);

        JavaPairInputDStream<String, String> messages = KafkaUtils.createStream(
                jssc,
                zkHost,
                "canova",
                Collections.singletonMap(kafkaTopic, 3));
        if(streamProcessor == null)
            streamProcessor = new Function<JavaPairRDD<String, String>, Void>() {
                @Override
                public Void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
                    Map<String, String> map = stringStringJavaPairRDD.collectAsMap();
                    if (!map.isEmpty()) {
                        Iterator<String> vals = map.values().iterator();
                        while (vals.hasNext()) {
                            try {
                                byte[] bytes = org.apache.commons.codec.binary.Base64.decodeBase64(vals.next());
                                Collection<Collection<Writable>> records = new RecordDeSerializer().deserialize("topic", bytes);
                                System.out.println(records);
                            } catch (Exception e) {
                                System.out.println("Error serializing");
                            }
                        }

                    }
                    return null;
                }
            };

        messages.foreach(streamProcessor);
    }


    /**
     * Run the pipeline
     * @throws Exception
     */
    public void run() throws Exception {
        // Start the computation
        camelContext.start();
        jssc.start();
        jssc.awaitTermination();
        camelContext.stop();

    }




}
