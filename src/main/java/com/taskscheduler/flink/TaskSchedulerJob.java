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
        
        // Set up Kafka source
        KafkaSource<Task> source = KafkaSource.<Task>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("flink-task-scheduler")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TaskDeserializationSchema())
                .build();

        // Create data stream from Kafka source
        DataStream<Task> tasks = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Task Events Source"
        );

        // Process tasks with scheduling logic
        DataStream<Task> scheduledTasks = tasks
                .keyBy(task -> task.getId().toString())
                .process(new TaskSchedulingFunction())
                .name("Task Scheduler Function");

        // Set up Kafka sink for scheduled tasks (only from onTimer)
        KafkaSink<Task> sink = KafkaSink.<Task>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new TaskSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Send scheduled tasks to Kafka (only when timer fires)
        scheduledTasks.sinkTo(sink).name("Scheduled Tasks Sink");

        // Also print for monitoring
        scheduledTasks.print().name("Task Monitor");

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

}
