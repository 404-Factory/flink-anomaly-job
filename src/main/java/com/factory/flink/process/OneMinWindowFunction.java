package com.factory.flink.process;

import com.factory.flink.dto.SensorReadingEvent;
import com.factory.flink.dto.SensorViolationEvent;
import com.factory.flink.process.engine.Rule3Engine;
import com.factory.flink.process.engine.RuleEngine;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class OneMinWindowFunction extends
    ProcessWindowFunction<SensorReadingEvent, SensorViolationEvent, String, TimeWindow> {

    private transient RuleEngine rule3Engine;

    @Override
    public void open(Configuration parameters) throws Exception {
        rule3Engine = new Rule3Engine();
    }

    @Override
    public void process(String s,
        ProcessWindowFunction<SensorReadingEvent, SensorViolationEvent, String, TimeWindow>.Context context,
        Iterable<SensorReadingEvent> elements, Collector<SensorViolationEvent> out)
        throws Exception {

    }
}
