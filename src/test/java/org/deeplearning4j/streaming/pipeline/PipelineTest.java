package org.deeplearning4j.streaming.pipeline;

import org.apache.spark.streaming.Durations;
import org.deeplearning4j.streaming.embedded.EmbeddedKafkaCluster;
import org.deeplearning4j.streaming.embedded.EmbeddedZookeeper;
import org.deeplearning4j.streaming.embedded.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by agibsonccc on 6/10/16.
 */
public class PipelineTest {
    private static  EmbeddedZookeeper zookeeper;
    private static EmbeddedKafkaCluster kafkaCluster;
    private static int zkPort;
    public final static String LOCALHOST = "localhost";

    @BeforeClass
    public static void init() throws Exception {
        zkPort = TestUtils.getAvailablePort();
        zookeeper = new EmbeddedZookeeper(zkPort);
        zookeeper.startup();
        kafkaCluster = new EmbeddedKafkaCluster(LOCALHOST + ":" + zkPort);
        kafkaCluster.startup();
    }

    @AfterClass
    public static void after() {
        kafkaCluster.shutdown();
        zookeeper.shutdown();
    }


    @Test
    public void testPipeline() throws Exception {
        SparkStreamingPipeline pipeline = SparkStreamingPipeline.builder()
                .dataType("csv").kafkaBroker(kafkaCluster.getBrokerList())
                .inputFormat("org.canova.api.formats.input.impl.ListStringInputFormat")
                .inputUri("file:src/test/resources/?fileName=iris.dat&noop=true").streamingDuration(Durations.seconds(1))
                .kafkaPartitions(1).kafkaTopic("test3").sparkMaster("local[*]").numLabels(3)
                .zkHost("localhost:" + zkPort).sparkAppName("canova").build();
        pipeline.init();
        pipeline.run();

    }


}
