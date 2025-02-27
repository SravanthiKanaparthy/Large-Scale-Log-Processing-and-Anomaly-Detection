package com.example.logprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;

import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import com.google.gson.JsonParser;
import com.google.gson.JsonObject;

public class LogProcessingJob {

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("group.id", "flink-log-processor");
        
        // Create Kafka consumer for logs topic
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
            "logs-stream", 
            new SimpleStringSchema(), 
            kafkaProps
        );
        kafkaConsumer.setStartFromLatest();
        
        // Create the data stream from Kafka
        DataStream<String> logStream = env.addSource(kafkaConsumer);
        
        // Parse JSON logs
        DataStream<JsonObject> parsedLogs = logStream.map(new MapFunction<String, JsonObject>() {
            @Override
            public JsonObject map(String value) throws Exception {
                return JsonParser.parseString(value).getAsJsonObject();
            }
        });
        
        // Filter for error logs
        DataStream<JsonObject> errorLogs = parsedLogs.filter(new FilterFunction<JsonObject>() {
            @Override
            public boolean filter(JsonObject log) throws Exception {
                return log.has("level") && 
                       log.get("level").getAsString().equalsIgnoreCase("ERROR");
            }
        });
        
        // Count errors by source
        DataStream<Tuple2<String, Integer>> errorCounts = errorLogs
            .map(new MapFunction<JsonObject, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(JsonObject log) throws Exception {
                    String source = log.has("source") ? 
                                   log.get("source").getAsString() : "unknown";
                    return new Tuple2<>(source, 1);
                }
            })
            .keyBy(tuple -> tuple.f0)
            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .sum(1);
        
        // Prepare for ML model input - enrich logs with additional features
        DataStream<JsonObject> enrichedLogs = parsedLogs.map(new MapFunction<JsonObject, JsonObject>() {
            @Override
            public JsonObject map(JsonObject log) throws Exception {
                // Add processing timestamp
                log.addProperty("processing_time", System.currentTimeMillis());
                
                // Extract log features for ML model
                if (log.has("message")) {
                    String message = log.get("message").getAsString();
                    log.addProperty("message_length", message.length());
                    log.addProperty("contains_error", message.toLowerCase().contains("error"));
                    log.addProperty("contains_exception", message.toLowerCase().contains("exception"));
                }
                
                return log;
            }
        });
        
        // Send enriched logs to Kafka for ML processing
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
            "ml-input-stream",
            new SimpleStringSchema(),
            kafkaProps
        );
        
        enrichedLogs.map(JsonObject::toString).addSink(kafkaProducer);
        
        // Configure Elasticsearch sink
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("elasticsearch", 9200, "http"));
        
        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
            httpHosts,
            new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {
                @Override
                public void process(Tuple2<String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
                    Map<String, Object> json = new HashMap<>();
                    json.put("source", element.f0);
                    json.put("error_count", element.f1);
                    json.put("timestamp", System.currentTimeMillis());
                    
                    IndexRequest request = Requests.indexRequest()
                        .index("error-metrics")
                        .source(json);
                    
                    indexer.add(request);
                }
            }
        );
        
        // Configuration for the Elasticsearch sink
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(
            restClientBuilder -> {
                restClientBuilder.setHttpClientConfigCallback(
                    httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(
                        new BasicCredentialsProvider() {{
                            setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials("elastic", "changeme")
                            );
                        }}
                    )
                );
            }
        );
        
        // Add the Elasticsearch sink
        errorCounts.addSink(esSinkBuilder.build());
        
        // Execute the Flink job
        env.execute("Log Processing and Anomaly Detection");
    }
}
