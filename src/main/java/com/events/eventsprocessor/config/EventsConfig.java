package com.events.eventsprocessor.config;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.dataflow.EPDataFlowInstance;
import com.espertech.esperio.amqp.AMQPSink;
import com.events.eventsprocessor.entity.User;
import com.events.eventsprocessor.event.TemperatureEvent;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventsConfig {

    @Value("${spring.rabbitmq.queue}")
    private String queue;

    @Value("${spring.rabbitmq.exchange}")
    private String exchange;

    @Value("${spring.rabbitmq.routingkey}")
    private String routingKey;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Value("${spring.rabbitmq.host}")
    private String host;

    @Value("${spring.rabbitmq.temperature.exchange}")
    private String temperatureExchange;

    @Value("${spring.rabbitmq.temperature.routingkey}")
    private String temperatureRoutingkey;

    @Value("${spring.rabbitmq.temperature.queue}")
    private String temperatureQueue;

    @Bean
    public EPServiceProvider epServiceProvider() {
        com.espertech.esper.client.Configuration config = new com.espertech.esper.client.Configuration();
        config.addEventTypeAutoName("com.events.eventsprocessor");
        config.addEventType(TemperatureEvent.class);
        EPServiceProvider epServiceProvider = EPServiceProviderManager.getDefaultProvider(config);
        return epServiceProvider;
    }

    @Bean
    public EPAdministrator epAdministrator() {
        return epServiceProvider().getEPAdministrator();
    }

    @Bean
    Queue queue() {
        return new Queue(queue, true);
    }

    @Bean
    Queue temperatureQueue() {
        return new Queue(temperatureQueue, true);
    }

    @Bean
    Exchange myExchange() {
        return ExchangeBuilder.directExchange(exchange).durable(true).build();
    }

    @Bean
    Exchange temperatureExchange() {
        return ExchangeBuilder.directExchange(temperatureExchange).durable(true).build();
    }

    @Bean
    Binding binding() {
        return BindingBuilder
                .bind(queue())
                .to(myExchange())
                .with(routingKey)
                .noargs();
    }

    @Bean
    Binding temperatureBinding() {
        return BindingBuilder
                .bind(temperatureQueue())
                .to(temperatureExchange())
                .with(temperatureRoutingkey)
                .noargs();
    }

    @Bean
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(host);
        cachingConnectionFactory.setUsername(username);
        cachingConnectionFactory.setPassword(password);
        return cachingConnectionFactory;
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jsonMessageConverter());
        return rabbitTemplate;
    }
}
