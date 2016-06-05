package org.deeplearning4j;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;

/**
 * A Camel Java DSL Router
 */
public class MyRouteBuilder extends RouteBuilder {

    /**
     * Let's configure the Camel routing rules using Java code...
     */
    public void configure() {
        from("direct:start").process(new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                exchange.getIn().setBody("Test Message from Camel Kafka Component Final",String.class);
                exchange.getIn().setHeader(KafkaConstants.PARTITION_KEY, 0);
                exchange.getIn().setHeader(KafkaConstants.KEY, "1");
            }
        }).to("kafka:localhost:9092?topic=test");

        from("kafka:localhost:9092?topic=test&groupId=testing&autoOffsetReset=earliest&consumersCount=1")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange)
                            throws Exception {
                        String messageKey = "";
                        if (exchange.getIn() != null) {
                            Message message = exchange.getIn();
                            Integer partitionId = (Integer) message
                                    .getHeader(KafkaConstants.PARTITION);
                            String topicName = (String) message
                                    .getHeader(KafkaConstants.TOPIC);
                            if (message.getHeader(KafkaConstants.KEY) != null)
                                messageKey = (String) message
                                        .getHeader(KafkaConstants.KEY);
                            Object data = message.getBody();


                            System.out.println("topicName :: "
                                    + topicName + " partitionId :: "
                                    + partitionId + " messageKey :: "
                                    + messageKey + " message :: "
                                    + data + "\n");
                        }
                    }
                }).to("log:input");

    }

}
