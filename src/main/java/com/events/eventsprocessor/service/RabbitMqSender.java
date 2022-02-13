package com.events.eventsprocessor.service;

import com.events.eventsprocessor.entity.User;
import com.events.eventsprocessor.event.TemperatureEvent;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class RabbitMqSender {
    private RabbitTemplate rabbitTemplate;

    @Autowired
    public RabbitMqSender(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @Value("${spring.rabbitmq.exchange}")
    private String exchange;

    @Value("${spring.rabbitmq.routingkey}")
    private String routingkey;

    @Value("${spring.rabbitmq.temperature.exchange}")
    private String temperatureExchange;

    @Value("${spring.rabbitmq.temperature.routingkey}")
    private String temperatureRoutingkey;

    public void send(User user) {
        rabbitTemplate.convertAndSend(exchange, routingkey, user);
    }

    public void send(TemperatureEvent temperatureEvent) {
        rabbitTemplate.convertAndSend(temperatureExchange, temperatureRoutingkey, temperatureEvent);

    }
}
