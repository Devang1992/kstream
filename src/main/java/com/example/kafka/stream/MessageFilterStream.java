package com.example.kafka.stream;

import com.example.kafka.processor.StatusFilterProcessor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class MessageFilterStream {

    private static final Logger logger = LoggerFactory.getLogger(MessageFilterStream.class);

    @Value("${kafka.topic.input:input-topic}")
    private String inputTopic;

    @Value("${kafka.topic.output:output-topic}")
    private String outputTopic;

    @Autowired
    private StatusFilterProcessor statusFilterProcessor;

    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        logger.info("Creating Kafka Streams topology - Input: {}, Output: {}", inputTopic, outputTopic);

        // Create the topology using Processor API for more control
        Topology topology = new Topology();

        // Add source processor
        topology.addSource("source", inputTopic);

        // Add our custom processor
        topology.addProcessor("status-filter-processor", 
            new ProcessorSupplier<String, String, String, String>() {
                @Override
                public StatusFilterProcessor get() {
                    return new StatusFilterProcessor();
                }
            }, "source");

        // Add sink processor
        topology.addSink("sink", outputTopic, "status-filter-processor");

        logger.info("Topology created successfully");
        return topology;
    }
}
