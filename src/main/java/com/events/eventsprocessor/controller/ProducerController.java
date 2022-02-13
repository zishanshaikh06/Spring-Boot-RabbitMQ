package com.events.eventsprocessor.controller;

import com.events.eventsprocessor.entity.User;
import com.events.eventsprocessor.service.RabbitMqSender;
import com.events.eventsprocessor.util.RandomTemperatureEventGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/api/v1/")
public class ProducerController {


    private RabbitMqSender rabbitMqSender;

    @Autowired
    public ProducerController(RabbitMqSender rabbitMqSender) {
        this.rabbitMqSender = rabbitMqSender;
    }

    @Autowired
    private RandomTemperatureEventGenerator randomTemperatureEventGenerator;

    @PostMapping(value = "user")
    public String publishUserDetails(@RequestBody User user) {
        rabbitMqSender.send(user);
        return "Message sent successfully!!";
    }

    @GetMapping(value = "events/generate")
    public String generate() {
        randomTemperatureEventGenerator.startSendingTemperatureReadings(1000);
        return "Events generation started successfully!!";
    }
}
