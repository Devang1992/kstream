package com.example.kafka.processor;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class StatusFilterProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context;

    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        this.context = context;
        log.info("StatusFilterProcessor initialized");
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        try {
            // Get the status header
            String status = null;
            if (record.headers() != null && record.headers().lastHeader("status") != null) {
                status = new String(record.headers().lastHeader("status").value());
            }

            log.debug("Processing record with key: {}, status header: {}", record.key(), status);

            // Only forward messages where status=completed
            if ("completed".equals(status)) {
                log.info("Forwarding message with key: {} - status is completed", record.key());
                context.forward(record);
            } else {
                log.debug("Filtering out message with key: {} - status is: {}", record.key(), status);
            }
        } catch (Exception e) {
            log.error("Error processing record with key: {}", record.key(), e);
        }
    }

    @Override
    public void close() {
        log.info("StatusFilterProcessor closed");
    }

    // Static method to create a copy of the processor (similar to your ToPdmProcessor.copy())
    public static StatusFilterProcessor copy(StatusFilterProcessor original) {
        return new StatusFilterProcessor();
    }
}
