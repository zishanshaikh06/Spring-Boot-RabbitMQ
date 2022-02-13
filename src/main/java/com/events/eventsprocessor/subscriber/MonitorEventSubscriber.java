package com.events.eventsprocessor.subscriber;

import java.util.Date;
import java.util.Map;

import com.events.eventsprocessor.event.TemperatureEvent;
import com.events.eventsprocessor.service.RabbitMqSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MonitorEventSubscriber implements StatementSubscriber {

    @Autowired
    private RabbitMqSender rabbitMqSender;

    private static Logger LOG = LoggerFactory.getLogger(MonitorEventSubscriber.class);

    public String getStatement() {
        return "select avg(temperature) as avg_val from TemperatureEvent.win:time_batch(5 sec)";
    }

    public void update(Map<String, Double> eventMap) {

        Double avg = (Double) eventMap.get("avg_val");
        StringBuilder sb = new StringBuilder();
        sb.append("---------------------------------");
        sb.append("\n- [MONITOR] Average Temp = " + avg);
        sb.append("\n---------------------------------");

        LOG.info(sb.toString());

        rabbitMqSender.send(new TemperatureEvent(avg, new Date()));
    }
}
