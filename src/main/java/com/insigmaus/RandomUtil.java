package com.insigmaus;



import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class RandomUtil {

    public static final String RANDOM_NUMBER = "0123456789";

    public static final String RANDOM_STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

    public static final Random RANDOM = new Random(System.currentTimeMillis());

    public static int getInt() {
        return Math.abs(RANDOM.nextInt());
    }

    public static long getLong() {
        return Math.abs(RANDOM.nextLong());
    }

    public static double getDouble() {
        return Math.abs(RANDOM.nextDouble());
    }

    public static float getFloat() {
        return RANDOM.nextFloat();
    }

    public static boolean getBoolean() {
        return RANDOM.nextBoolean();
    }

    public static String getString() {
        return UUID.randomUUID().toString();
    }

    public static String getDigitString(int len) {
        return getString(RANDOM_NUMBER, len);
    }

    public static String getString(int len) {
        return getString(RANDOM_STRING, len);
    }

    public static byte getByte() {
        return Integer.valueOf(getInt()).byteValue();
    }

    public static short getShort() {
        return (short) RANDOM.nextInt(Short.MAX_VALUE);
    }

    public static Date getDate() {
        return new Date();
    }

    public static Timestamp getTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static String getString(Random random, CharSequence seekStr, int len) {
        StringBuffer buf = new StringBuffer(len);
        for (int i = 0; i < len; i++) {
            int idx = random.nextInt(seekStr.length());
            buf.append(seekStr.charAt(idx));
        }
        return buf.toString();
    }

    public static String getString(CharSequence seekStr, int len) {
        return getString(RANDOM, seekStr, len);
    }

    public static int getInt(int n) {
        return RANDOM.nextInt(n);
    }

}

