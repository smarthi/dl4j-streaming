package org.deeplearning4j.streaming.routes;

import lombok.AllArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.commons.codec.binary.Base64;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.io.ByteArrayInputStream;

/**
 * Serve results from a kafka queue.
 *
 * @author Adam Gibson
 */
@AllArgsConstructor
public class DL4jServeRouteBuilder extends RouteBuilder {
    private String modelUri;
    private String kafkaBroker;
    private String consumingTopic;
    private boolean computationGraph;
    private String outputUri;

    /**
     * <b>Called on initialization to build the routes using the fluent builder syntax.</b>
     * <p/>
     * This is a central method for RouteBuilder implementations to implement
     * the routes using the Java fluent builder syntax.
     *
     * @throws Exception can be thrown during configuration
     */
    @Override
    public void configure() throws Exception {
        from(String.format("kafka:%s?topic=%s",
                kafkaBroker,
                consumingTopic)).process(new Processor() {
            @Override
            public void process(Exchange exchange) throws Exception {
                String base64NDArray = (String) exchange.getIn().getBody();
                byte[] arr  = Base64.decodeBase64(base64NDArray);
                ByteArrayInputStream bis = new ByteArrayInputStream(arr);
                INDArray predict = Nd4j.read(bis);
                if(computationGraph) {
                    ComputationGraph graph = ModelSerializer.restoreComputationGraph(modelUri);
                    INDArray[] output = graph.output(predict);
                    exchange.getOut().setBody(output);

                }
                else {
                    MultiLayerNetwork network = ModelSerializer.restoreMultiLayerNetwork(modelUri);
                    INDArray output = network.output(predict);
                    exchange.getOut().setBody(output);
                }


            }
        }).to(outputUri);
    }
}
