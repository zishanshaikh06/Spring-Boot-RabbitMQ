package com.events.eventsprocessor.util;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.events.eventsprocessor.event.TemperatureEvent;
import com.events.eventsprocessor.handler.TemperatureEventAMQPAdapterHandler;
import com.events.eventsprocessor.handler.TemperatureEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
public class RandomTemperatureEventGenerator {

    private static Logger LOG = LoggerFactory.getLogger(RandomTemperatureEventGenerator.class);

    @Autowired
    private TemperatureEventHandler temperatureEventHandler;

    @Autowired
    private TemperatureEventAMQPAdapterHandler temperatureEventAMQPAdapterHandler;

    public void startSendingTemperatureReadings(final long noOfTemperatureEvents, String handler) {

        ExecutorService xrayExecutor = Executors.newSingleThreadExecutor();

        xrayExecutor.submit(new Runnable() {
            public void run() {

                LOG.debug(getStartingMessage());

                int count = 0;
                while (count < noOfTemperatureEvents) {
                    double start = 0;
                    double end = 500;
                    double random = new Random().nextDouble();
                    double result = start + (random * (end - start));
                    TemperatureEvent ve = new TemperatureEvent(result, new Date());
                    if("amqp".equals(handler)){
                        temperatureEventAMQPAdapterHandler.handle(ve);
                    } else {
                        temperatureEventHandler.handle(ve);
                    }
                    count++;
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        LOG.error("Thread Interrupted", e);
                    }
                }

            }
        });
    }


    private String getStartingMessage(){
        StringBuilder sb = new StringBuilder();
        sb.append("\n\n************************************************************");
        sb.append("\n* STARTING - ");
        sb.append("\n* PLEASE WAIT - TEMPERATURES ARE RANDOM SO MAY TAKE");
        sb.append("\n* A WHILE TO SEE WARNING AND CRITICAL EVENTS!");
        sb.append("\n************************************************************\n");
        return sb.toString();
    }
}
