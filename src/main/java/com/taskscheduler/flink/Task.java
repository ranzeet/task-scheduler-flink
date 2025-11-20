package com.taskscheduler.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Task {
    private UUID id;
    private String name;
    private String description;
    private String status;
    private String cronExpression;
    private Instant nextExecutionTime;
    private Instant createdAt;
    private Instant updatedAt;
    private Map<String, String> parameters;
    private String createdBy;
    private String assignedTo;
    private int retryCount;
    private int currentRetries;
    private int maxRetries;
    private long retryDelayMs;
    private String executionResult;
    private String errorMessage;
}
