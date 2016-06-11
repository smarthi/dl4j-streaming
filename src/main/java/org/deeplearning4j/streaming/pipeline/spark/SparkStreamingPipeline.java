package org.deeplearning4j.streaming.pipeline.spark;

import lombok.Builder;
import lombok.Data;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.canova.api.writable.Writable;
import org.deeplearning4j.streaming.conversion.dataset.RecordToDataSet;
import org.deeplearning4j.streaming.pipeline.kafka.BaseKafkaPipeline;
import org.deeplearning4j.streaming.routes.CamelKafkaRouteBuilder;
import org.deeplearning4j.streaming.serde.RecordSerializer;
import org.nd4j.linalg.dataset.DataSet;

import java.util.*;

/**
 * Spark streaming pipeline.
 *
 */
@Data
public class SparkStreamingPipeline extends BaseKafkaPipeline<JavaDStream<DataSet>> {
    protected JavaStreamingContext jssc;
    protected SparkConf sparkConf;
    protected Function<JavaPairRDD<String, String>, Void> streamProcessor;
    protected Duration streamingDuration =  Durations.seconds(1);
    protected String sparkMaster;

    @Builder
    public SparkStreamingPipeline(String kafkaTopic, String inputUri, String inputFormat, String kafkaBroker, String zkHost, CamelContext camelContext, String hadoopHome, String dataType, String sparkAppName, int kafkaPartitions, RecordToDataSet recordToDataSetFunction, int numLabels, JavaDStream<DataSet> dataset, JavaStreamingContext jssc, SparkConf sparkConf, Function<JavaPairRDD<String, String>, Void> streamProcessor, Duration streamingDuration, String sparkMaster) {
        super(kafkaTopic, inputUri, inputFormat, kafkaBroker, zkHost, camelContext, hadoopHome, dataType, sparkAppName, kafkaPartitions, recordToDataSetFunction, numLabels, dataset);
        this.jssc = jssc;
        this.sparkConf = sparkConf;
        this.streamProcessor = streamProcessor;
        this.streamingDuration = streamingDuration;
        this.sparkMaster = sparkMaster;
    }

    @Override
    public void initComponents() {
        sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster);
        jssc = new JavaStreamingContext(sparkConf, streamingDuration);
    }

    /**
     * Create the streaming result
     *
     * @return the stream
     */
    @Override
    public JavaDStream<DataSet> createStream() {
        JavaPairInputDStream<String, String> messages = KafkaUtils.createStream(
                jssc,
                zkHost,
                "canova",
                Collections.singletonMap(kafkaTopic, kafkaPartitions));
        JavaDStream<DataSet> dataset = messages.flatMap(new DataSetFlatmap(numLabels,recordToDataSetFunction)).cache();
        dataset.foreach(new Function<JavaRDD<DataSet>, Void>() {
            @Override
            public Void call(JavaRDD<DataSet> dataSetJavaRDD) throws Exception {
                return null;
            }
        });
        jssc.start();
        jssc.awaitTermination();
        return dataset;
    }
}
