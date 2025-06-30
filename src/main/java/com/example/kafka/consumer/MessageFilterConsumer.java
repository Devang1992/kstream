package com.example.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.function.Consumer;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class MessageFilterConsumer {

    private final KafkaTemplate<String, JsonNode> kafkaTemplate;

    @Bean
    public Consumer<KStream<String, JsonNode>> processMessages() {
        return input -> {
            log.info("Message processing stream started");
            
            input.foreach((key, value) -> {
                try {
                    // In Spring Cloud Stream, we need to handle headers differently
                    // For now, let's assume the status is in the message payload
                    String status = extractStatusFromMessage(value);
                    
                    if ("completed".equals(status)) {
                        log.info("Processing message with key: {} - status is completed", key);
                        
                        // Send to output topic
                        kafkaTemplate.send("filtered-messages-topic", key, value);
                        log.info("Sent filtered message to output topic");
                    } else {
                        log.debug("Filtering out message with key: {} - status is: {}", key, status);
                    }
                } catch (Exception e) {
                    log.error("Error processing message with key: {}", key, e);
                }
            });
        };
    }
    
    private String extractStatusFromMessage(JsonNode message) {
        if (message != null && message.has("status")) {
            return message.get("status").asText();
        }
        return null;
    }
}
