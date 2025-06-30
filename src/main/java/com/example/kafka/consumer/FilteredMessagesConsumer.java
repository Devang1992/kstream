package com.example.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FilteredMessagesConsumer {

    @KafkaListener(topics = "filtered-messages-topic", groupId = "filtered-messages-consumer-group")
    public void consumeFilteredMessage(
            ConsumerRecord<String, JsonNode> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Consumed filtered message from topic: {}, partition: {}, offset: {}", 
                    topic, partition, record.offset());
            log.info("Message key: {}", record.key());
            log.info("Message value: {}", record.value());
            
            // Log all headers
            if (record.headers() != null) {
                log.info("Message headers:");
                for (Header header : record.headers()) {
                    log.info("  {}: {}", header.key(), new String(header.value()));
                }
            }

            // Process the filtered message
            processFilteredMessage(record.key(), record.value(), record.headers());

            // Acknowledge the message
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            
        } catch (Exception e) {
            log.error("Error processing filtered message: {}", e.getMessage(), e);
        }
    }

    private void processFilteredMessage(String key, JsonNode value, org.apache.kafka.common.header.Headers headers) {
        log.info("Processing filtered message with key: {} and value: {}", key, value);
        
        // Add your business logic here
        // Example: Save to database, call external API, etc.
        
        log.info("Successfully processed filtered message");
    }
}
