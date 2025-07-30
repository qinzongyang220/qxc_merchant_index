package com.zy.util;

import com.zy.model.UserInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 用户数据生成器
 */
public class UserDataGenerator {
    
    private static final Random random = new Random();
    private static final String[] DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "163.com", "qq.com"};
    private static final String[] PASSWORD_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()".split("");
    
    /**
     * 生成指定数量的用户数据
     * @param count 数量
     * @return 用户数据列表
     */
    public static List<UserInfo> generateUserData(int count) {
        List<UserInfo> users = new ArrayList<>(count);
        
        for (int i = 0; i < count; i++) {
            UserInfo user = new UserInfo();
            user.setId((long) (i + 1));
            user.setUsername("user_" + random.nextInt(10000));
            user.setPassword(generateRandomPassword(8 + random.nextInt(8)));
            user.setEmail(user.getUsername() + "@" + DOMAINS[random.nextInt(DOMAINS.length)]);
            user.setPhone("1" + (3 + random.nextInt(6)) + generateRandomNumbers(9));
            
            users.add(user);
        }
        
        return users;
    }
    
    /**
     * 生成随机密码
     */
    private static String generateRandomPassword(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(PASSWORD_CHARS[random.nextInt(PASSWORD_CHARS.length)]);
        }
        return sb.toString();
    }
    
    /**
     * 生成指定长度的随机数字
     */
    private static String generateRandomNumbers(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(random.nextInt(10));
        }
        return sb.toString();
    }
} 