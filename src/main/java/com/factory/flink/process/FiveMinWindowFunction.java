package com.factory.flink.process;

import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.process.engine.BiasEngine;
import com.factory.flink.process.engine.Rule1Engine;
import com.factory.flink.process.engine.RuleEngine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FiveMinWindowFunction extends
    ProcessWindowFunction<SensorReadingEvent, SensorViolationEvent, String, TimeWindow> {

    private transient RuleEngine rule1Engine;
    private transient RuleEngine biasEngine;

    @Override
    public void open(Configuration parameters) throws Exception {
        rule1Engine = new Rule1Engine();
        biasEngine = new BiasEngine();
    }

    @Override
    public void process(String s,
        ProcessWindowFunction<SensorReadingEvent, SensorViolationEvent, String, TimeWindow>.Context context,
        Iterable<SensorReadingEvent> elements, Collector<SensorViolationEvent> out)
        throws Exception {

        // rule1Engine.evaluate();
        // biasEngine.evaluate();
        // rule1Engine.emitTransition();
        // biasEngine.emitTransition();
    }
}
