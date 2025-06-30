package com.example.kafka.controller;

import com.example.kafka.producer.TestMessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestController {

    private final TestMessageProducer testMessageProducer;

    @PostMapping("/send-completed")
    public String sendCompletedMessage(@RequestParam String message) {
        testMessageProducer.sendCompletedMessage(message);
        return "Sent completed message: " + message;
    }

    @PostMapping("/send-pending")
    public String sendPendingMessage(@RequestParam String message) {
        testMessageProducer.sendPendingMessage(message);
        return "Sent pending message: " + message;
    }

    @PostMapping("/send-in-progress")
    public String sendInProgressMessage(@RequestParam String message) {
        testMessageProducer.sendInProgressMessage(message);
        return "Sent in-progress message: " + message;
    }

    @PostMapping("/send-custom")
    public String sendCustomMessage(@RequestParam String message, @RequestParam String status) {
        testMessageProducer.sendMessageWithStatus(message, status);
        return "Sent message with status " + status + ": " + message;
    }
}
