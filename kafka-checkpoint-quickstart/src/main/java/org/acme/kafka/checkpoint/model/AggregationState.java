package org.acme.kafka.checkpoint.model;

import java.util.HashMap;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class AggregationState extends HashMap<String, Aggregation> {
    
}
