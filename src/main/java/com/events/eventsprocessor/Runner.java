package com.events.eventsprocessor;

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
        randomTemperatureEventGenerator.startSendingTemperatureReadings(1000);
    }
}
