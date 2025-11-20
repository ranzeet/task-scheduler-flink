package com.taskscheduler.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskMetaData {
    private Long bucketId;
    private String id;
    private String tenant;
    private Long scheduledAt;
    private String status;
    
    // Constructor for easy creation
    public TaskMetaData() {}
    
    public TaskMetaData(String id, String tenant, Long scheduledAt, String status) {
        this.id = id;
        this.tenant = tenant;
        this.scheduledAt = scheduledAt;
        this.status = status;
    }
}
