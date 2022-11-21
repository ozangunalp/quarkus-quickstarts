package org.acme.kafka.checkpoint.model;

import java.time.Instant;

public class TemperatureMeasurement {

    public int stationId;
    public String stationName;
    public Instant timestamp;
    public double value;

    public TemperatureMeasurement(int stationId, String stationName, Instant timestamp, double value) {
        this.stationId = stationId;
        this.stationName = stationName;
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public String toString() {
        return "TemperatureMeasurement{" +
                "stationId=" + stationId +
                ", stationName='" + stationName + '\'' +
                ", timestamp=" + timestamp +
                ", value=" + value +
                '}';
    }
}
