package com.factory.flink.process.engine;

import com.factory.flink.dto.RuleResult;
import com.factory.flink.dto.SensorReadingEvent;
import java.util.List;

public interface RuleEngine {

    RuleResult evaluate(List<SensorReadingEvent> samples,
        Double recipeMin,
        Double recipeMax);
}
