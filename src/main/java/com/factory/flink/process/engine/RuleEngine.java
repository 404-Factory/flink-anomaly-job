package com.factory.flink.process.engine;

import java.util.List;

import com.factory.flink.domain.dto.RuleResult;
import com.factory.flink.domain.dto.SensorReadingEvent;

public interface RuleEngine {

    RuleResult evaluate(List<SensorReadingEvent> samples,
        Double recipeMin,
        Double recipeMax);
}
