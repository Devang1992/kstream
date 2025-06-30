package com.example.kafka.consumer;

import com.example.kafka.processor.StatusFilterProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.kafka.core.KafkaStreamsTracing;
import org.springframework.stereotype.Component;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
@Component
public class StatusFilterConsumer implements Consumer<KStream<String, JsonNode>> {

    private final StatusFilterProcessor statusFilterProcessor;
    private final KafkaStreamsTracing kafkaStreamsTracing;

    @Override
    public void accept(KStream<String, JsonNode> input) {
        KStream<String, JsonNode> stream;
        
        if (kafkaStreamsTracing != null) {
            stream = input
                .process(kafkaStreamsTracing.process("transform", () -> StatusFilterProcessor.copy(statusFilterProcessor)));
        } else {
            stream = input
                .process(() -> StatusFilterProcessor.copy(statusFilterProcessor));
        }
        
        // Send filtered messages to output topic
        stream.to("filtered-messages-topic");
        
        log.info("StatusFilterConsumer configured successfully");
    }
}
