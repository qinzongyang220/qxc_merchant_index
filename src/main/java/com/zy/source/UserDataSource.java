//package com.zy.source;
//
//import com.zy.model.UserInfo;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//import java.util.Random;
//import java.util.concurrent.TimeUnit;
//
///**
// * 用户数据源生成器
// */
//public class UserDataSource implements SourceFunction<UserInfo> {
//
//    private boolean isRunning = true;
//    private final Random random = new Random();
//    private static final String[] DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "163.com", "qq.com"};
//    private static final String[] PASSWORD_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()".split("");
//
//    @Override
//    public void run(SourceContext<UserInfo> ctx) throws Exception {
//        long id = 1L;
//
//        while (isRunning) {
//            // 生成随机用户信息
//            UserInfo user = new UserInfo();
//            user.setId(id++);
//            user.setUsername("user_" + random.nextInt(10000));
//            user.setPassword(generateRandomPassword(8 + random.nextInt(8)));
//            user.setEmail(user.getUsername() + "@" + DOMAINS[random.nextInt(DOMAINS.length)]);
//            user.setPhone("1" + (3 + random.nextInt(6)) + generateRandomNumbers(9));
//
//            // 发送数据
//            ctx.collect(user);
//
//            // 控制生成速率
//            TimeUnit.MILLISECONDS.sleep(500 + random.nextInt(500));
//        }
//    }
//
//    @Override
//    public void cancel() {
//        isRunning = false;
//    }
//
//    /**
//     * 生成随机密码
//     */
//    private String generateRandomPassword(int length) {
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < length; i++) {
//            sb.append(PASSWORD_CHARS[random.nextInt(PASSWORD_CHARS.length)]);
//        }
//        return sb.toString();
//    }
//
//    /**
//     * 生成指定长度的随机数字
//     */
//    private String generateRandomNumbers(int length) {
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < length; i++) {
//            sb.append(random.nextInt(10));
//        }
//        return sb.toString();
//    }
//}