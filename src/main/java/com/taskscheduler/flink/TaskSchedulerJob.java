package com.taskscheduler.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Flink Job for Task Scheduling with Kafka Integration
 * Reads from task-requests topic and writes to scheduled-tasks topic
 */
public class TaskSchedulerJob {
    private static final Logger log = LoggerFactory.getLogger(TaskSchedulerJob.class);
    public static void main(String[] args) throws Exception {
        // Configuration from arguments or defaults
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String inputTopic = args.length > 1 ? args[1] : "task-requests";
        String outputTopic = args.length > 2 ? args[2] : "scheduled-tasks";

        log.info("Starting Task Scheduler Flink Job with Kafka {}", System.currentTimeMillis());
        log.info("Bootstrap Servers: {} {}", bootstrapServers, System.currentTimeMillis());
        log.info("Input Topic: {} {}", inputTopic, System.currentTimeMillis());
        log.info("Output Topic: {} {}", outputTopic, System.currentTimeMillis());

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing
        env.enableCheckpointing(10000); // 10 seconds
        
        // Set up Kafka source for TaskMetaData
        KafkaSource<TaskMetaData> source = KafkaSource.<TaskMetaData>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TaskMetaDataDeserializationSchema())
                .build();

        // Create data stream from Kafka source
        DataStream<TaskMetaData> taskMetaData = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "TaskMetaData Events Source"
        );

        // Process task metadata with scheduling logic
        DataStream<TaskMetaData> scheduledTaskMetaData = taskMetaData
                .keyBy(metadata -> metadata.getId())
                .process(new TaskMetaDataSchedulingFunction())
                .name("TaskMetaData Scheduler Function");

        // Set up Kafka sink for scheduled task metadata (only from onTimer)
        KafkaSink<TaskMetaData> sink = KafkaSink.<TaskMetaData>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new TaskMetaDataSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Send scheduled task metadata to Kafka (only when timer fires)
        scheduledTaskMetaData.sinkTo(sink).name("Scheduled TaskMetaData Sink");

        // Also print for monitoring
        scheduledTaskMetaData.print().name("TaskMetaData Monitor");

        // Execute the job
        env.execute("Task Scheduler Job - Kafka Integration");
        
        log.info("Task Scheduler Flink Job completed");
    }

    /**
     * Task Scheduling Function with State Management
     */
    public static class TaskSchedulingFunction extends KeyedProcessFunction<String, Task, Task> {
        private transient ValueState<Long> nextExecutionTimeState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            log.info("[OPEN METHOD] Call to open method at {}", System.currentTimeMillis());
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "next-execution-time",
                    TypeInformation.of(Long.class)
            );
            nextExecutionTimeState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Task task, Context ctx, Collector<Task> out) throws Exception {
            long receiveTimestamp = System.currentTimeMillis();
            log.info("[INBOUND] [{}] Received task from task-requests topic: {} with status: {} at {}",
                    receiveTimestamp, task.getId(), task.getStatus(), Instant.ofEpochMilli(receiveTimestamp));

            // Calculate next execution time based on cron expression
            // For demo purposes, schedule 5 minutes from now
            long now = System.currentTimeMillis();
            long nextExecutionTime = now + 300000; // 5 minute later

            log.info("[PROCESSING] [{}] Calculating timer for task: {} - scheduling for {} (5 minutes from now)",
                    System.currentTimeMillis(), task.getId(), Instant.ofEpochMilli(nextExecutionTime));

            // Update state
            nextExecutionTimeState.update(nextExecutionTime);

            // Register timer for next execution
            ctx.timerService().registerProcessingTimeTimer(nextExecutionTime);

            log.info("[PROCESSING] [{}] Timer registered for task: {} at timestamp: {}",
                    System.currentTimeMillis(), task.getId(), nextExecutionTime);

            // Update task status (no outbound publishing)
            task.setStatus("SCHEDULED_BY_FLINK");
            task.setNextExecutionTime(Instant.ofEpochMilli(nextExecutionTime));
            task.setUpdatedAt(Instant.now());

            log.info("[PROCESSING-COMPLETE] [{}] Task {} processed and scheduled for execution at {} - NO OUTBOUND PUBLISHING",
                    System.currentTimeMillis(), task.getId(), Instant.ofEpochMilli(nextExecutionTime));

            log.info("[SUMMARY] Task {} processed in {}ms - timer set for {}",
                    task.getId(), (System.currentTimeMillis() - receiveTimestamp), Instant.ofEpochMilli(nextExecutionTime));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Task> out) throws Exception {
            long timerFireTimestamp = System.currentTimeMillis();
            log.info("[TIMER-FIRED] [{}] Timer fired for task key: {} at scheduled timestamp: {} (actual: {})", 
                    timerFireTimestamp, ctx.getCurrentKey(), Instant.ofEpochMilli(timestamp), Instant.ofEpochMilli(timerFireTimestamp));
            
            Long nextTime = nextExecutionTimeState.value();
            
            if (nextTime != null && nextTime == timestamp) {
                log.info("[TIMER-PROCESSING] [{}] Processing timer for task: {} - creating execution task", 
                        System.currentTimeMillis(), ctx.getCurrentKey());
                
                // Create task for execution
                Task task = new Task();
                task.setId(java.util.UUID.fromString(ctx.getCurrentKey()));
                task.setStatus("READY_FOR_EXECUTION");
                task.setUpdatedAt(Instant.ofEpochMilli(timestamp));
                
                long beforeTimerOutboundTimestamp = System.currentTimeMillis();
                log.info("[TIMER-OUTBOUND-BEFORE] [{}] About to publish timer-triggered task: {} to scheduled-tasks topic with status: {}", 
                        beforeTimerOutboundTimestamp, task.getId(), task.getStatus());
                
                // Emit task for execution (THIS IS WHERE WE PUBLISH)
                out.collect(task);
                
                long afterTimerOutboundTimestamp = System.currentTimeMillis();
                log.info("[TIMER-OUTBOUND-AFTER] [{}] Successfully published timer-triggered task: {} to scheduled-tasks topic (took {}ms)", 
                        afterTimerOutboundTimestamp, task.getId(), (afterTimerOutboundTimestamp - beforeTimerOutboundTimestamp));
                
                // Clear state - no more recurring executions
                nextExecutionTimeState.clear();
                
                log.info("[TIMER-COMPLETED] [{}] Task {} execution completed - no more timers scheduled", 
                        System.currentTimeMillis(), task.getId());
            } else {
                log.warn("[TIMER-MISMATCH] [{}] Timer timestamp {} does not match expected {} for task: {}", 
                        System.currentTimeMillis(), timestamp, nextTime, ctx.getCurrentKey());
            }
        }
    }

    /**
     * Task Deserialization Schema
     */
    public static class TaskDeserializationSchema implements DeserializationSchema<Task> {
        private transient ObjectMapper objectMapper;

        @Override
        public Task deserialize(byte[] message) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .findAndRegisterModules()
                        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }
            
            long deserializeTimestamp = System.currentTimeMillis();
            try {
                Task task = objectMapper.readValue(message, Task.class);
                log.info("[DESERIALIZE] [{}] Successfully deserialized task: {} from Kafka message", 
                        deserializeTimestamp, task != null ? task.getId() : "null");
                return task;
            } catch (Exception e) {
                log.error("[DESERIALIZE-ERROR] [{}] Failed to deserialize task from Kafka message: {}", 
                        deserializeTimestamp, e.getMessage(), e);
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(Task nextElement) {
            return false;
        }

        @Override
        public TypeInformation<Task> getProducedType() {
            return TypeInformation.of(Task.class);
        }
    }

    /**
     * Task Serialization Schema
     */
    public static class TaskSerializationSchema implements SerializationSchema<Task> {
        private transient ObjectMapper objectMapper;

        @Override
        public byte[] serialize(Task task) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .findAndRegisterModules()
                        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }
            
            long serializeTimestamp = System.currentTimeMillis();
            try {
                byte[] serializedData = objectMapper.writeValueAsBytes(task);
                log.info("[SERIALIZE] [{}] Successfully serialized task: {} to {} bytes for Kafka", 
                        serializeTimestamp, task.getId(), serializedData.length);
                return serializedData;
            } catch (Exception e) {
                log.error("[SERIALIZE-ERROR] [{}] Failed to serialize task: {} - {}", 
                        serializeTimestamp, task.getId(), e.getMessage(), e);
                return new byte[0];
            }
        }
    }

    /**
     * TaskMetaData Scheduling Function with State Management
     */
    public static class TaskMetaDataSchedulingFunction extends KeyedProcessFunction<String, TaskMetaData, TaskMetaData> {
        private ValueState<Long> nextExecutionTimeState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            log.info("[OPEN METHOD] TaskMetadata scheduling function opened at {}", System.currentTimeMillis());
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "next-execution-time",
                    TypeInformation.of(Long.class)
            );
            nextExecutionTimeState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TaskMetaData taskMetaData, Context ctx, Collector<TaskMetaData> out) throws Exception {
            long receiveTimestamp = System.currentTimeMillis();
            log.info("[INBOUND] [{}] Received TaskMetaData from task-requests topic: {} with status: {} at {}",
                    receiveTimestamp, taskMetaData.getId(), taskMetaData.getStatus(), Instant.ofEpochMilli(receiveTimestamp));

            // Use scheduledAt from taskMetaData, with fallback logic
            long now = System.currentTimeMillis();
            long nextExecutionTime;
            
            if (taskMetaData.getScheduledAt() != null && taskMetaData.getScheduledAt() > now) {
                // Use the provided scheduledAt time if it's in the future
                nextExecutionTime = taskMetaData.getScheduledAt();
                log.info("[PROCESSING] [{}] Using provided scheduledAt for task: {} - scheduling for {}", 
                        System.currentTimeMillis(), taskMetaData.getId(), Instant.ofEpochMilli(nextExecutionTime));
            } else {
                // Fallback: schedule 30 seconds from now if scheduledAt is missing/past
                nextExecutionTime = now + 30000; // 30 seconds later as fallback
                log.info("[PROCESSING] [{}] Using fallback scheduling for task: {} - scheduledAt was {} - scheduling for {} (30 seconds from now)", 
                        System.currentTimeMillis(), taskMetaData.getId(), 
                        taskMetaData.getScheduledAt() != null ? Instant.ofEpochMilli(taskMetaData.getScheduledAt()) : "null",
                        Instant.ofEpochMilli(nextExecutionTime));
            }

            // Update state
            nextExecutionTimeState.update(nextExecutionTime);

            // Register timer for next execution
            ctx.timerService().registerProcessingTimeTimer(nextExecutionTime);

            log.info("[PROCESSING] [{}] Timer registered for task: {} at timestamp: {}",
                    System.currentTimeMillis(), taskMetaData.getId(), nextExecutionTime);

            // Update task status (no outbound publishing in processElement)
            taskMetaData.setStatus("SCHEDULED_BY_FLINK");

            log.info("[PROCESSING-COMPLETE] [{}] TaskMetaData {} processed and scheduled for execution at {} - NO OUTBOUND PUBLISHING",
                    System.currentTimeMillis(), taskMetaData.getId(), Instant.ofEpochMilli(nextExecutionTime));

            log.info("[SUMMARY] TaskMetaData {} processed in {}ms - timer set for {}",
                    taskMetaData.getId(), (System.currentTimeMillis() - receiveTimestamp), Instant.ofEpochMilli(nextExecutionTime));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<TaskMetaData> out) throws Exception {
            long timerFireTimestamp = System.currentTimeMillis();
            log.info("[TIMER-FIRED] [{}] Timer fired for task key: {} at scheduled timestamp: {} (actual: {})", 
                    timerFireTimestamp, ctx.getCurrentKey(), Instant.ofEpochMilli(timestamp), Instant.ofEpochMilli(timerFireTimestamp));
            
            Long nextTime = nextExecutionTimeState.value();
            
            if (nextTime != null && nextTime == timestamp) {
                log.info("[TIMER-PROCESSING] [{}] Processing timer for task: {} - creating execution TaskMetadata", 
                        System.currentTimeMillis(), ctx.getCurrentKey());
                
                // Create TaskMetaData for execution
                TaskMetaData taskMetaData = new TaskMetaData();
                taskMetaData.setId(ctx.getCurrentKey()); // String ID
                taskMetaData.setStatus("READY_FOR_EXECUTION");
                taskMetaData.setScheduledAt(timestamp);
                
                long beforeTimerOutboundTimestamp = System.currentTimeMillis();
                log.info("[TIMER-OUTBOUND-BEFORE] [{}] About to publish timer-triggered TaskMetaData: {} to scheduled-tasks topic with status: {}", 
                        beforeTimerOutboundTimestamp, taskMetaData.getId(), taskMetaData.getStatus());
                
                // Emit TaskMetaData for execution (THIS IS WHERE WE PUBLISH)
                out.collect(taskMetaData);
                
                long afterTimerOutboundTimestamp = System.currentTimeMillis();
                log.info("[TIMER-OUTBOUND-AFTER] [{}] Successfully published timer-triggered TaskMetaData: {} to scheduled-tasks topic (took {}ms)", 
                        afterTimerOutboundTimestamp, taskMetaData.getId(), (afterTimerOutboundTimestamp - beforeTimerOutboundTimestamp));
                
                // Clear state - no more recurring executions
                nextExecutionTimeState.clear();
                
                log.info("[TIMER-COMPLETED] [{}] TaskMetaData {} execution completed - no more timers scheduled", 
                        System.currentTimeMillis(), taskMetaData.getId());
            } else {
                log.warn("[TIMER-MISMATCH] [{}] Timer timestamp {} does not match expected {} for task: {}", 
                        System.currentTimeMillis(), timestamp, nextTime, ctx.getCurrentKey());
            }
        }
    }

    /**
     * TaskMetaData Deserialization Schema
     */
    public static class TaskMetaDataDeserializationSchema implements DeserializationSchema<TaskMetaData> {
        private transient ObjectMapper objectMapper;

        @Override
        public TaskMetaData deserialize(byte[] message) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .findAndRegisterModules()
                        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }
            
            long deserializeTimestamp = System.currentTimeMillis();
            try {
                TaskMetaData taskMetaData = objectMapper.readValue(message, TaskMetaData.class);
                log.info("[DESERIALIZE] [{}] Successfully deserialized TaskMetaData: {} from Kafka message", 
                        deserializeTimestamp, taskMetaData != null ? taskMetaData.getId() : "null");
                return taskMetaData;
            } catch (Exception e) {
                log.error("[DESERIALIZE-ERROR] [{}] Failed to deserialize TaskMetaData from Kafka message: {}", 
                        deserializeTimestamp, e.getMessage(), e);
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(TaskMetaData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<TaskMetaData> getProducedType() {
            return TypeInformation.of(TaskMetaData.class);
        }
    }

    /**
     * TaskMetaData Serialization Schema
     */
    public static class TaskMetaDataSerializationSchema implements SerializationSchema<TaskMetaData> {
        private transient ObjectMapper objectMapper;

        @Override
        public byte[] serialize(TaskMetaData taskMetaData) {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper()
                        .findAndRegisterModules()
                        .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            }
            
            long serializeTimestamp = System.currentTimeMillis();
            try {
                byte[] serializedData = objectMapper.writeValueAsBytes(taskMetaData);
                log.info("[SERIALIZE] [{}] Successfully serialized TaskMetaData: {} to {} bytes for Kafka", 
                        serializeTimestamp, taskMetaData.getId(), serializedData.length);
                return serializedData;
            } catch (Exception e) {
                log.error("[SERIALIZE-ERROR] [{}] Failed to serialize TaskMetaData: {} - {}", 
                        serializeTimestamp, taskMetaData.getId(), e.getMessage(), e);
                return new byte[0];
            }
        }
    }

}
