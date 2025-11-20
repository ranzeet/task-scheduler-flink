package com.taskscheduler.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
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

        log.info("Starting Task Scheduler Flink Job with Kafka {}", System.currentTimeMillis());
        log.info("Bootstrap Servers: {} {}", bootstrapServers, System.currentTimeMillis());
        log.info("Input Topic: {} {}", inputTopic, System.currentTimeMillis());
        log.info("Mode: Consumer Only - No Outbound Publishing {}", System.currentTimeMillis());

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

        // Process tasks with scheduling logic (no outbound Kafka publishing)
        tasks
                .keyBy(task -> task.getId().toString())
                .process(new TaskSchedulingFunction())
                .name("Task Scheduler Function")
                .print().name("Task Monitor"); // Only print for monitoring

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
                
                log.info("[TIMER-EXECUTION] [{}] Timer executed for task: {} - TASK READY FOR EXECUTION (no outbound publishing)", 
                        System.currentTimeMillis(), ctx.getCurrentKey());
                
                // Clear state - no more recurring executions
                nextExecutionTimeState.clear();
                
                log.info("[TIMER-COMPLETED] [{}] Task {} execution timer completed - no more timers scheduled", 
                        System.currentTimeMillis(), ctx.getCurrentKey());
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

}
