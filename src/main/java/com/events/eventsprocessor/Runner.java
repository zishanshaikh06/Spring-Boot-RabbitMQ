package com.events.eventsprocessor;

import com.events.eventsprocessor.handler.TemperatureEventHandler;
import com.events.eventsprocessor.util.RandomTemperatureEventGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class Runner implements CommandLineRunner {

    @Autowired
    RandomTemperatureEventGenerator randomTemperatureEventGenerator;

    private static Logger LOG = LoggerFactory.getLogger(Runner.class);

    @Override
    public void run(String... args) throws Exception {
        System.out.println(args);
        LOG.info("Starting runner... args {}", args);
        String handler = args.length > 0 ? args[0] : "";
        randomTemperatureEventGenerator.startSendingTemperatureReadings(1000, handler);
    }
}
