package com.example.kafka.controller;

import com.example.kafka.producer.MessageProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducer messageProducer;

    @PostMapping("/send-completed")
    public String sendCompletedMessage(@RequestParam String content) {
        messageProducer.sendCompletedMessage(content);
        return "Sent completed message: " + content;
    }

    @PostMapping("/send-pending")
    public String sendPendingMessage(@RequestParam String content) {
        messageProducer.sendPendingMessage(content);
        return "Sent pending message: " + content;
    }

    @PostMapping("/send")
    public String sendMessage(@RequestParam String content, @RequestParam String status) {
        messageProducer.sendMessage(content, status);
        return "Sent message with status " + status + ": " + content;
    }
}
