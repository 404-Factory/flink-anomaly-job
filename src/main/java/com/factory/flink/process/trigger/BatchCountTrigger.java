package com.factory.flink.process.trigger;

import com.factory.flink.dto.SensorReadingEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class BatchCountTrigger extends Trigger<SensorReadingEvent, TimeWindow> {

    @Override
    public TriggerResult onElement(SensorReadingEvent element, long timestamp, TimeWindow window,
        TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
        throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
        throws Exception {
        return null;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }
}
