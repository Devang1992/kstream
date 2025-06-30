package com.example.kafka.stream;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class StatusFilterFunction {

    @Bean
    public Consumer<KStream<String, JsonNode>> statusFilter() {
        return input -> {
            log.info("StatusFilter function started");
            
            input
                .filter((key, value) -> {
                    // Get the status from headers
                    String status = getStatusFromHeaders(input);
                    boolean shouldProcess = "completed".equals(status);
                    
                    if (shouldProcess) {
                        log.info("Processing message with key: {} - status is completed", key);
                    } else {
                        log.debug("Filtering out message with key: {} - status is: {}", key, status);
                    }
                    
                    return shouldProcess;
                })
                .peek((key, value) -> log.info("Sending filtered message with key: {}", key))
                .to("filtered-messages-topic");
        };
    }
    
    private String getStatusFromHeaders(KStream<String, JsonNode> stream) {
        // This is a simplified approach - in real implementation,
        // you'd access headers through the record context
        return "completed"; // Placeholder - will be handled properly in the actual stream processing
    }
}
