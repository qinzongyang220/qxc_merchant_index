//package com.zy;
//
//import com.zy.model.UserInfo;
//import com.zy.source.UserDataSource;
//import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
//import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
//import org.apache.flink.connector.jdbc.JdbcSink;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
///**
// * Flink MySQL 写入程序
// * 将随机生成的用户数据写入MySQL数据库
// */
//public class FlinkMySQLWriter {
//
//    // MySQL连接配置
//    private static final String JDBC_URL = "jdbc:mysql://rm-2zer55j9y09b13451.mysql.rds.aliyuncs.com:3306/bigdata_center?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
//    private static final String USERNAME = "bigdata_center";
//    private static final String PASSWORD = "Dx2025^&A!";
//
//    // 插入SQL
//    private static final String INSERT_SQL =
//            "INSERT INTO user_info (username, password, email, phone) VALUES (?, ?, ?, ?)";
//
//    public static void main(String[] args) throws Exception {
//        // 创建执行环境
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 设置并行度
//        env.setParallelism(4);
//
//        // 添加数据源
//        DataStream<UserInfo> userDataStream = env.addSource(new UserDataSource());
//
//        // 打印数据流（用于调试）
//        userDataStream.print();
//
//        // 创建JDBC连接和执行选项
//        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//            .withUrl(JDBC_URL)
//            .withDriverName("com.mysql.cj.jdbc.Driver")
//            .withUsername(USERNAME)
//            .withPassword(PASSWORD)
//            .build();
//
//        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
//            .withBatchSize(100)          // 批处理大小
//            .withBatchIntervalMs(200)    // 批处理时间间隔
//            .withMaxRetries(3)           // 最大重试次数
//            .build();
//
//        // 配置JDBC Sink - 使用Flink 1.17 API
//        userDataStream.addSink(
//            JdbcSink.sink(
//                INSERT_SQL,
//                (statement, user) -> {
//                    // 设置参数
//                    statement.setString(1, user.getUsername());
//                    statement.setString(2, user.getPassword());
//                    statement.setString(3, user.getEmail());
//                    statement.setString(4, user.getPhone());
//                },
//                executionOptions,
//                connectionOptions
//            )
//        );
//
//        System.out.println("开始执行Flink作业...");
//        System.out.println("数据将被写入MySQL数据库的user_info表中");
//
//        // 执行作业
//        env.execute("Flink MySQL Writer Job");
//    }
//}