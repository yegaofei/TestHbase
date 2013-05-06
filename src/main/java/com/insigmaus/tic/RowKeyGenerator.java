package com.insigmaus.tic;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 26, 2013
 */

public class RowKeyGenerator {

    private static final int BUCKET_SIZE = 700720;

    public static int getBucketSize() {
        return BUCKET_SIZE;
    }

    public static int generateRowKey(int startKey, int offset) {
        int rowKey = startKey + offset;

        // rowKey = symbolId * BUCKET_SIZE + offset; // 800000 can make sure
        // enough
        // // space for tic data, no rowkey
        // // will be duplicated
        //
        if (rowKey < 0) {
            throw new RuntimeException("The rowKey outbound of Integer rang");
        }

        return rowKey;
    }


    public static void main(String[] args) {
        String s = "$TSECON";

        int hashCode = s.hashCode();
        System.out.println(s + "'s hash code is " + hashCode);

        long startTime = 1367062371763L;
        long timeStamp = 1367069031088L;

        System.out.println(timeStamp - startTime);

        long x = BUCKET_SIZE * 190009; // 1683344640
        System.out.println(x);
        long m = x / 190009;
        System.out.println(m);

    }
}


