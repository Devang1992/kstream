package com.example.kafka.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, JsonNode> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    private static final String INPUT_TOPIC = "input-messages-topic";

    public void sendMessage(String content, String status) {
        try {
            String key = UUID.randomUUID().toString();
            
            Map<String, Object> messageData = new HashMap<>();
            messageData.put("id", key);
            messageData.put("content", content);
            messageData.put("status", status);
            messageData.put("timestamp", System.currentTimeMillis());
            
            JsonNode message = objectMapper.valueToTree(messageData);
            
            kafkaTemplate.send(INPUT_TOPIC, key, message);
            log.info("Sent message with key: {}, status: {}, content: {}", key, status, content);
            
        } catch (Exception e) {
            log.error("Error sending message: {}", e.getMessage(), e);
        }
    }
    
    public void sendCompletedMessage(String content) {
        sendMessage(content, "completed");
    }
    
    public void sendPendingMessage(String content) {
        sendMessage(content, "pending");
    }
}
