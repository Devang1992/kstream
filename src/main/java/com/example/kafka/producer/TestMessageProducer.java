package com.example.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
public class TestMessageProducer {

    @Autowired
    private KafkaTemplate<String, JsonNode> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String INPUT_TOPIC = "input-messages-topic";

    public void sendMessageWithStatus(String messageContent, String status) {
        try {
            String key = UUID.randomUUID().toString();
            
            // Create JSON message
            Map<String, Object> messageMap = new HashMap<>();
            messageMap.put("id", key);
            messageMap.put("content", messageContent);
            messageMap.put("timestamp", System.currentTimeMillis());
            
            JsonNode jsonMessage = objectMapper.valueToTree(messageMap);
            
            ProducerRecord<String, JsonNode> record = new ProducerRecord<>(INPUT_TOPIC, key, jsonMessage);
            record.headers().add(new RecordHeader("status", status.getBytes()));
            
            kafkaTemplate.send(record);
            log.info("Sent message with key: {}, status: {}, content: {}", key, status, messageContent);
            
        } catch (Exception e) {
            log.error("Error sending message: {}", e.getMessage(), e);
        }
    }

    public void sendCompletedMessage(String messageContent) {
        sendMessageWithStatus(messageContent, "completed");
    }

    public void sendPendingMessage(String messageContent) {
        sendMessageWithStatus(messageContent, "pending");
    }

    public void sendInProgressMessage(String messageContent) {
        sendMessageWithStatus(messageContent, "in-progress");
    }
}
