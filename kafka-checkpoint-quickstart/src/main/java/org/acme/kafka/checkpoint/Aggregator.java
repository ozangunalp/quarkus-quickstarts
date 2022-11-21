package org.acme.kafka.checkpoint;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

import org.acme.kafka.checkpoint.model.Aggregation;
import org.acme.kafka.checkpoint.model.AggregationState;
import org.acme.kafka.checkpoint.model.TemperatureMeasurement;
import org.acme.kafka.checkpoint.model.WeatherStation;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.smallrye.reactive.messaging.kafka.commit.CheckpointMetadata;

@ApplicationScoped
public class Aggregator {

    private static final Logger LOG = Logger.getLogger(Aggregator.class);

    static final String WEATHER_STATIONS_TOPIC = "weather-stations";
    static final String WEATHER_STATIONS_TABLE = "weather-stations-table";
    static final String TEMPERATURE_VALUES_TOPIC = "temperature-values";
    static final String TEMPERATURES_AGGREGATED_TOPIC = "temperatures-aggregated";

    @Incoming(WEATHER_STATIONS_TOPIC)
    @Outgoing(WEATHER_STATIONS_TABLE)
    public Message<Map<String, String>> stationsTable(Message<WeatherStation> msg) {
        CheckpointMetadata<Map<String, String>> checkpoint = CheckpointMetadata.fromMessage(msg);
        WeatherStation station = msg.getPayload();
        checkpoint.transform(HashMap::new, s -> {
            s.put(String.valueOf(station.id), station.name);
            return s;
        });
        return msg.withPayload(checkpoint.getNext().getState());
    }

    @Channel(WEATHER_STATIONS_TABLE)
    Multi<Map<String, String>> weatherStations;
    
    @Incoming(TEMPERATURE_VALUES_TOPIC)
    @Outgoing(TEMPERATURES_AGGREGATED_TOPIC)
    public Multi<Message<Aggregation>> aggregate(Multi<KafkaRecord<Integer, String>> measurements) {
        return Multi.createBy().combining().streams(measurements, weatherStations)
                .latestItems()
                .using((measurementMessage, stations) -> {
                    String timestampAndValue = measurementMessage.getPayload();
                    Integer key = measurementMessage.getKey();
                    String[] parts = timestampAndValue.split(";");
                    String stationName = stations.get(String.valueOf(key));
                    return measurementMessage.withPayload(new TemperatureMeasurement(key, stationName, 
                            Instant.parse(parts[0]), Double.valueOf(parts[1])));
                })
                .filter(m -> m.getPayload().stationName != null)
                .map(t -> {
                    CheckpointMetadata<AggregationState> checkpoint = CheckpointMetadata.fromMessage(t);
                    TemperatureMeasurement measurement = t.getPayload();
                    String stationId = String.valueOf(measurement.stationId);
                    checkpoint.transform(AggregationState::new, aggregations -> {
                        aggregations.compute(stationId, (k, a) -> {
                            if (a == null) {
                                a = new Aggregation();
                            }
                            return a.updateFrom(measurement);
                        });
                        return aggregations;
                    });
                    Aggregation aggregation = checkpoint.getNext().getState().get(stationId);
                    LOG.infov("station: {0}, average: {1}, count: {2}, total: {3}", 
                            aggregation.stationName, aggregation.avg, aggregation.count, aggregation.sum);
                    return t.withPayload(aggregation);
                });
    }
    
}
