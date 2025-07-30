//package com.zy;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//import java.util.Properties;
//
///**
// * Flink Kafka 消费者程序
// * 从Kafka的canal_topic_mix主题读取数据
// */
//public class FlinkKafkaReader {
//
//    // Kafka配置
//    private static final String KAFKA_BROKERS = "kafka01:9092";
//    private static final String KAFKA_TOPIC = "canal_topic_mix";
//    private static final String CONSUMER_GROUP_ID = "flink-kafka-consumer-group";
//
//    public static void main(String[] args) throws Exception {
//        // 创建执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置并行度
//        env.setParallelism(1);
//
//        // 配置Kafka消费者
//        Properties kafkaProps = new Properties();
//        kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS);
//        kafkaProps.setProperty("group.id", CONSUMER_GROUP_ID);
//        // 从最早的记录开始读取
//        kafkaProps.setProperty("auto.offset.reset", "earliest");
//
//        // 创建Kafka消费者
//        FlinkKafkaConsumer<String> kafkaConsumer =
//            new FlinkKafkaConsumer<>(KAFKA_TOPIC, new SimpleStringSchema(), kafkaProps);
//
//        // 添加Kafka数据源
//        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);
//
//        // 打印原始消息
//        kafkaStream.print("原始Kafka消息");
//
//        // 解析JSON数据
//        DataStream<JSONObject> jsonStream = kafkaStream
//            .map(new MapFunction<String, JSONObject>() {
//                @Override
//                public JSONObject map(String value) throws Exception {
//                    try {
//                        return JSON.parseObject(value);
//                    } catch (Exception e) {
//                        System.err.println("JSON解析错误: " + e.getMessage());
//                        System.err.println("原始消息: " + value);
//                        return new JSONObject();
//                    }
//                }
//            });
//
//        // 打印解析后的JSON数据
//        jsonStream.map(json -> "解析后的JSON: " + json.toJSONString()).print();
//
//        System.out.println("开始执行Flink Kafka消费者作业...");
//        System.out.println("从Kafka主题 " + KAFKA_TOPIC + " 读取数据");
//
//        // 执行作业
//        env.execute("Flink Kafka Consumer Job");
//    }
//}