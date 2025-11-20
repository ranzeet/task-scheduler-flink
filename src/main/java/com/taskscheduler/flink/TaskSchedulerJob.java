package com.taskscheduler.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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
        
        log.info("Starting Task Scheduler Flink Job with Kafka");
        log.info("Bootstrap Servers: {}", bootstrapServers);
        log.info("Input Topic: {}", inputTopic);
        log.info("Output Topic: {}", outputTopic);

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

        // Set up Kafka sink for scheduled tasks
        KafkaSink<Task> sink = KafkaSink.<Task>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new TaskSerializationSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Send scheduled tasks to Kafka
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
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "next-execution-time",
                    TypeInformation.of(Long.class)
            );
            nextExecutionTimeState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Task task, Context ctx, Collector<Task> out) throws Exception {
            log.info("Processing task: {} with status: {}", task.getId(), task.getStatus());
            
            // Calculate next execution time based on cron expression
            // For demo purposes, schedule 1 minute from now
            long now = System.currentTimeMillis();
            long nextExecutionTime = now + 60000; // 1 minute later
            
            // Update state
            nextExecutionTimeState.update(nextExecutionTime);
            
            // Register timer for next execution
            ctx.timerService().registerProcessingTimeTimer(nextExecutionTime);
            
            // Update task status and emit
            task.setStatus("SCHEDULED_BY_FLINK");
            task.setNextExecutionTime(Instant.ofEpochMilli(nextExecutionTime));
            task.setUpdatedAt(Instant.now());
            
            out.collect(task);
            
            log.info("Task {} scheduled for execution at {}", 
                    task.getId(), Instant.ofEpochMilli(nextExecutionTime));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Task> out) throws Exception {
            log.info("Timer fired for task key: {} at timestamp: {}", ctx.getCurrentKey(), timestamp);
            
            Long nextTime = nextExecutionTimeState.value();
            
            if (nextTime != null && nextTime == timestamp) {
                // Create task for execution
                Task task = new Task();
                task.setId(java.util.UUID.fromString(ctx.getCurrentKey()));
                task.setStatus("READY_FOR_EXECUTION");
                task.setUpdatedAt(Instant.ofEpochMilli(timestamp));
                
                // Emit task for execution
                out.collect(task);
                
                // Schedule next execution (for recurring tasks)
                long newNextTime = timestamp + 60000; // Next execution in 1 minute
                nextExecutionTimeState.update(newNextTime);
                ctx.timerService().registerProcessingTimeTimer(newNextTime);
                
                log.info("Task {} ready for execution, next scheduled at {}", 
                        task.getId(), Instant.ofEpochMilli(newNextTime));
            }
        }
    }

    /**
     * Task Deserialization Schema
     */
    public static class TaskDeserializationSchema implements DeserializationSchema<Task> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Task deserialize(byte[] message) {
            try {
                return objectMapper.readValue(message, Task.class);
            } catch (Exception e) {
                log.error("Failed to deserialize task", e);
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
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] serialize(Task task) {
            try {
                return objectMapper.writeValueAsBytes(task);
            } catch (Exception e) {
                log.error("Failed to serialize task", e);
                return new byte[0];
            }
        }
    }
}
