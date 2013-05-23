package com.insigmaus.tic;

import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

public interface TicDataWriter extends Runnable {

    byte[] COLUMN_TIME = Bytes.toBytes("t");
    byte[] COLUMN_FLAGS = Bytes.toBytes("f");
    byte[] COLUMN_EXH_TIME = Bytes.toBytes("e");
    byte[] COLUMN_SEQUENCENUMBER = Bytes.toBytes("s");
    byte[] COLUMN_LINEID = Bytes.toBytes("l");
    byte[] COLUMN_SEQUENCESEIRES = Bytes.toBytes("ss");
    byte[] COLUMN_SEQUENCEQAULIFIER = Bytes.toBytes("sq");
    byte[] COLUMN_TRADEEXCHANGE = Bytes.toBytes("te");
    byte[] COLUMN_TRADEPRICE = Bytes.toBytes("p");
    byte[] COLUMN_VWAP = Bytes.toBytes("v");
    byte[] COLUMN_TRADEVOLUME = Bytes.toBytes("tv");
    byte[] COLUMN_QUALIFIERS = Bytes.toBytes("q");
    byte[] COLUMN_CUMVOLUME = Bytes.toBytes("c");
    byte[] COLUMN_VOLQUALIFIERS = Bytes.toBytes("vq");
    byte[] COLUMN_ASK_EXCHANGE = Bytes.toBytes("a");
    byte[] COLUMN_BID_EXCHANGE = Bytes.toBytes("b");
    byte[] COLUMN_BID_PRICE = Bytes.toBytes("bp");
    byte[] COLUMN_ASK_PRICE = Bytes.toBytes("ap");
    byte[] COLUMN_BID_SIZE = Bytes.toBytes("bz");
    byte[] COLUMN_ASK_SIZE = Bytes.toBytes("az");

    int HTABLE_BUFFER_SIZE = 1024 * 1024 * 100;

    byte[] FAMILY_NAME = Bytes.toBytes("cf");

    String TIC_TRADE_TABLE_NAME = "Tic_Trade";

    String TIC_QUOTE_TABLE_NAME = "Tic_Quote";

    public void setConf(Configuration conf);

    public void setCountDownLatch(CountDownLatch countDownLatch);

    public void setSymbolCountArray(SymbolCount[] symbolCountArray);

}


