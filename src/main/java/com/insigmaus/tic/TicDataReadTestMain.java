package com.insigmaus.tic;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

public class TicDataReadTestMain {

    private Configuration conf;

    private static final int PROCESS_COUNT = 3;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private CountDownLatch countDownLatch;

    public TicDataReadTestMain(Configuration conf) {
        this.conf = conf;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        TicDataReadTestMain tdrt = new TicDataReadTestMain(HBaseConfiguration.create());
        tdrt.fetchRecordBySymbol(SymbolLoader.getSymbolList());
    }

    private void fetchRecordBySymbol(List<String> symbolList) {

        int symbolListSize = symbolList.size();
        int segementSize = symbolListSize / PROCESS_COUNT;
        while ((segementSize * PROCESS_COUNT) < symbolListSize) {
            segementSize++;
        }

        countDownLatch = new CountDownLatch(PROCESS_COUNT);

        for (int i = 0; i < PROCESS_COUNT; i++) {
            int startIndex = i * segementSize;
            int endIndex = startIndex + segementSize;

            if (endIndex > symbolList.size()) {
                endIndex = symbolList.size();
            }
            List<String> subSymbolList = symbolList.subList(startIndex, endIndex);
            TicDataReader tdr = new TicDataReader(this.conf, subSymbolList);
            tdr.setCountDownLatch(countDownLatch);
            Thread t = new Thread(tdr);
            t.start();
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}


