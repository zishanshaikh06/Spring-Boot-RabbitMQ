package com.espertech.esperio.amqp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EsperAMQPSerializer implements ObjectToAMQPCollector {
    @Override
    public void collect(ObjectToAMQPCollectorContext context) {
        try {
            context.getEmitter().send(new ObjectMapper().writeValueAsBytes(context.getObject()));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
