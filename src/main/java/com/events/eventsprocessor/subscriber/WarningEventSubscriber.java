package com.events.eventsprocessor.subscriber;

import com.events.eventsprocessor.event.TemperatureEvent;
import com.events.eventsprocessor.service.RabbitMqSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class WarningEventSubscriber implements StatementSubscriber {

    @Autowired
    private RabbitMqSender rabbitMqSender;

    private static Logger LOG = LoggerFactory.getLogger(WarningEventSubscriber.class);

    private static final String WARNING_EVENT_THRESHOLD = "400";


    public String getStatement() {
        String warningEventExpression = "select * from TemperatureEvent "
                + "match_recognize ( "
                + "       measures A as temp1, B as temp2 "
                + "       pattern (A B) "
                + "       define "
                + "               A as A.temperature > " + WARNING_EVENT_THRESHOLD + ", "
                + "               B as B.temperature > " + WARNING_EVENT_THRESHOLD + ")";

        return warningEventExpression;
    }

    public void update(Map<String, TemperatureEvent> eventMap) {
        TemperatureEvent temp1 = eventMap.get("temp1");
        TemperatureEvent temp2 = eventMap.get("temp2");

        StringBuilder sb = new StringBuilder();
        sb.append("--------------------------------------------------");
        sb.append("\n- [WARNING] : TEMPERATURE SPIKE DETECTED = " + temp1 + "," + temp2);
        sb.append("\n--------------------------------------------------");

        LOG.info(sb.toString());

        rabbitMqSender.send(temp1);
        rabbitMqSender.send(temp2);
    }
}