package com.events.eventsprocessor.event;

import java.util.Date;

public class TemperatureEvent {
    /** Temperature in Celcius. */
    private Double temperature;

    /** Time temerature reading was taken. */
    private Date timeOfReading;

    /**
     * Single value constructor.
     * @param value Temperature in Celsius.
     */
    /**
     * Temeratur constructor.
     * @param temperature Temperature in Celsius
     * @param timeOfReading Time of Reading
     */
    public TemperatureEvent(Double temperature, Date timeOfReading) {
        this.temperature = temperature;
        this.timeOfReading = timeOfReading;
    }

    /**
     * Get the Temperature.
     * @return Temperature in Celsius
     */
    public Double getTemperature() {
        return temperature;
    }

    /**
     * Get time Temperature reading was taken.
     * @return Time of Reading
     */
    public Date getTimeOfReading() {
        return timeOfReading;
    }

    @Override
    public String toString() {
        return "TemperatureEvent [" + temperature + "C]";
    }
}
