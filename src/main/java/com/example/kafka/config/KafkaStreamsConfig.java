package com.example.kafka.config;

import com.example.kafka.consumer.StatusFilterConsumer;
import com.example.kafka.processor.StatusFilterProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.KafkaStreamsTracing;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class KafkaStreamsConfig {

    @Bean
    public Consumer<KStream<String, JsonNode>> statusFilterConsumer(
            StatusFilterProcessor statusFilterProcessor,
            @Autowired(required = false) KafkaStreamsTracing kafkaStreamsTracing) {
        return new StatusFilterConsumer(statusFilterProcessor, kafkaStreamsTracing);
    }

    @Bean
    public StatusFilterProcessor statusFilterProcessor() {
        return new StatusFilterProcessor();
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanCustomizer(
            @Autowired(required = false) KafkaStreamsTracing kafkaStreamsTracing) {
        return factoryBean -> {
            if (kafkaStreamsTracing != null) {
                factoryBean.setClientSupplier(kafkaStreamsTracing.kafkaClientSupplier());
            }
        };
    }
}
