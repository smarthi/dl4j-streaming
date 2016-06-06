package org.deeplearning4j;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

/**
 * A Camel Java DSL Router
 */
public class CamelKafkaRouteBuilder extends RouteBuilder {
    private String topicName;
    private int kafkaPort;
    private String kafkaHostName;

    public CamelKafkaRouteBuilder(String topicName, int kafkaPort, String kafkaHostName) {
        this.topicName = topicName;
        this.kafkaPort = kafkaPort;
        this.kafkaHostName = kafkaHostName;
    }

    public CamelKafkaRouteBuilder(CamelContext context, String topicName, int kafkaPort, String kafkaHostName) {
        super(context);
        this.topicName = topicName;
        this.kafkaPort = kafkaPort;
        this.kafkaHostName = kafkaHostName;
    }

    public CamelKafkaRouteBuilder(String topicName, int kafkaPort) {
        this(topicName,kafkaPort,"localhost");
    }

    public CamelKafkaRouteBuilder(CamelContext context, String topicName, int kafkaPort) {
        this(context, topicName, kafkaPort,"localhost");
    }

    /**
     * Let's configure the Camel routing rules using Java code...
     */
    @Override
    public void configure() {
        from("file:src/test/resources/?fileName=iris.dat&noop=true")
                .unmarshal().csv()
                .to("canova://org.canova.api.formats.input.impl.ListStringInputFormat?inputMarshaller=org.canova.camel.component.csv.marshaller.ListStringInputMarshaller&writableConverter=org.canova.api.io.converters.SelfWritableConverter")
                .to(String.format("kafka:%s:%d?topic=%s",kafkaHostName,kafkaPort,topicName));


    }

}
