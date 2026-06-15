package com.factory.flink.domain.dto;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import com.factory.flink.domain.enums.AnomalyType;
import com.factory.flink.domain.enums.RuleName;
import com.factory.flink.domain.enums.Severity;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RuleResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean detected;
    private RuleName ruleName;
    private Severity severity;
    private AnomalyType anomalyType;
    private Double measuredValue;
    private Double referenceValue;
    private Double deviation;
    private Double deviationRate;
    private String reason;

    public static RuleResult normal() {
        return RuleResult.builder().detected(false).build();
    }

    public static RuleResult detected(RuleName ruleName, Severity severity, AnomalyType anomalyType,
                                     Double measuredValue, Double referenceValue, Double deviation, Double deviationRate, String reason) {
        return RuleResult.builder()
                .detected(true)
                .ruleName(ruleName)
                .severity(severity)
                .anomalyType(anomalyType)
                .measuredValue(measuredValue)
                .referenceValue(referenceValue)
                .deviation(deviation)
                .deviationRate(deviationRate)
                .reason(reason)
                .build();
    }
}
