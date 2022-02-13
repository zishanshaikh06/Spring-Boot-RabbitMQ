package com.events.eventsprocessor.subscriber;

import java.util.Map;

import com.events.eventsprocessor.event.TemperatureEvent;
import com.events.eventsprocessor.service.RabbitMqSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CriticalEventSubscriber implements StatementSubscriber {

    @Autowired
    private RabbitMqSender rabbitMqSender;

    private static Logger LOG = LoggerFactory.getLogger(CriticalEventSubscriber.class);

    private static final String CRITICAL_EVENT_THRESHOLD = "100";

    private static final String CRITICAL_EVENT_MULTIPLIER = "1.5";

    public String getStatement() {

        String crtiticalEventExpression = "select * from TemperatureEvent "
                + "match_recognize ( "
                + "       measures A as temp1, B as temp2, C as temp3, D as temp4 "
                + "       pattern (A B C D) "
                + "       define "
                + "               A as A.temperature > " + CRITICAL_EVENT_THRESHOLD + ", "
                + "               B as (A.temperature < B.temperature), "
                + "               C as (B.temperature < C.temperature), "
                + "               D as (C.temperature < D.temperature) and D.temperature > (A.temperature * " + CRITICAL_EVENT_MULTIPLIER + ")" + ")";

        return crtiticalEventExpression;
    }

    public void update(Map<String, TemperatureEvent> eventMap) {

        TemperatureEvent temp1 = eventMap.get("temp1");
        TemperatureEvent temp2 = eventMap.get("temp2");
        TemperatureEvent temp3 = eventMap.get("temp3");
        TemperatureEvent temp4 = eventMap.get("temp4");

        StringBuilder sb = new StringBuilder();
        sb.append("***************************************");
        sb.append("\n* [ALERT] : CRITICAL EVENT DETECTED! ");
        sb.append("\n* " + temp1 + " > " + temp2 + " > " + temp3 + " > " + temp4);
        sb.append("\n***************************************");

        LOG.info(sb.toString());

        rabbitMqSender.send(temp1);
        rabbitMqSender.send(temp2);
        rabbitMqSender.send(temp3);
        rabbitMqSender.send(temp4);
    }
}