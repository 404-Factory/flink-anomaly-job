package com.factory.flink.process.engine;

import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import java.util.List;

public class Rule1Engine implements RuleEngine {

    @Override
    public RuleResult evaluate(List<SensorReadingEvent> samples, Double recipeMin,
        Double recipeMax) {
        return null;
    }
}
