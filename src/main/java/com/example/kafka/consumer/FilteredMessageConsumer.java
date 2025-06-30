package com.example.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FilteredMessageConsumer {

    @KafkaListener(topics = "filtered-messages-topic", groupId = "filtered-consumer-group")
    public void handleFilteredMessage(
            ConsumerRecord<String, JsonNode> record,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("Received filtered message from topic: {}, partition: {}, offset: {}", 
                    topic, partition, record.offset());
            log.info("Message key: {}, value: {}", record.key(), record.value());
            
            // Process the filtered message
            processMessage(record.key(), record.value());
            
            // Acknowledge the message
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
            
        } catch (Exception e) {
            log.error("Error handling filtered message: {}", e.getMessage(), e);
        }
    }
    
    private void processMessage(String key, JsonNode value) {
        log.info("Processing filtered message with key: {}", key);
        
        // Add your business logic here
        // Example: Save to database, call external service, etc.
        
        log.info("Successfully processed message");
    }
}
