package com.insigma.tickserver;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

public class WRFDataWriter implements Runnable {

    static final byte[] COLUMN_DWPRESIGNATURE = Bytes.toBytes("d");
    static final byte[] COLUMN_RECORDTYPE = Bytes.toBytes("r");
    static final byte[] COLUMN_UNUSED = Bytes.toBytes("u");
    static final byte[] COLUMN_RECORDLENGTH = Bytes.toBytes("re");
    static final byte[] COLUMN_SEQUENCE = Bytes.toBytes("s");
    static final byte[] COLUMN_SYMBOL = Bytes.toBytes("sy");
    static final byte[] COLUMN_DWPOSTSIGNATURE = Bytes.toBytes("dw");

    /* TickDataType TickData */
    static final byte[] COLUMN_FLAGS = Bytes.toBytes("f");
    static final byte[] COLUMN_SEQUENCESERIES = Bytes.toBytes("ss");
    static final byte[] COLUMN_CATEGORY = Bytes.toBytes("c");
    static final byte[] COLUMN_SUBCATEGORY = Bytes.toBytes("sc");
    static final byte[] COLUMN_LINEID = Bytes.toBytes("l");
    static final byte[] COLUMN_AUTHCODE = Bytes.toBytes("a");
    static final byte[] COLUMN_EXCHANGETIME = Bytes.toBytes("e");
    static final byte[] COLUMN_BEACON = Bytes.toBytes("b");
    static final byte[] COLUMN_VWAP = Bytes.toBytes("v");
    static final byte[] COLUMN_SEQUENCENUMBER = Bytes.toBytes("sn");
    static final byte[] COLUMN_SECQUALIFIERS = Bytes.toBytes("sq");

    /* PriceDataType Trade */
    static final byte[] COLUMN_TRADE_VALUE = Bytes.toBytes("tv");
    static final byte[] COLUMN_TRADE_SIZE = Bytes.toBytes("ts");
    static final byte[] COLUMN_TRADE_UCUMVOLUME = Bytes.toBytes("tu");
    static final byte[] COLUMN_TRADE_FLAGS = Bytes.toBytes("tf");
    static final byte[] COLUMN_TRADE_QUALIFIERS = Bytes.toBytes("tq");
    static final byte[] COLUMN_TRADE_VOLQUALIFIERS = Bytes.toBytes("to");
    static final byte[] COLUMN_TRADE_EXCHANGE = Bytes.toBytes("te");

    /* PriceDataType Bid */
    static final byte[] COLUMN_BID_VALUE = Bytes.toBytes("bv");
    static final byte[] COLUMN_BID_SIZE = Bytes.toBytes("bs");
    static final byte[] COLUMN_BID_UCUMVOLUME = Bytes.toBytes("bu");
    static final byte[] COLUMN_BID_FLAGS = Bytes.toBytes("bf");
    static final byte[] COLUMN_BID_QUALIFIERS = Bytes.toBytes("bq");
    static final byte[] COLUMN_BID_VOLQUALIFIERS = Bytes.toBytes("bo");
    static final byte[] COLUMN_BID_EXCHANGE = Bytes.toBytes("be");

    /* PriceDataType Ask */
    static final byte[] COLUMN_ASK_VALUE = Bytes.toBytes("av");
    static final byte[] COLUMN_ASK_SIZE = Bytes.toBytes("as");
    static final byte[] COLUMN_ASK_UCUMVOLUME = Bytes.toBytes("au");
    static final byte[] COLUMN_ASK_FLAGS = Bytes.toBytes("af");
    static final byte[] COLUMN_ASK_QUALIFIERS = Bytes.toBytes("aq");
    static final byte[] COLUMN_ASK_VOLQUALIFIERS = Bytes.toBytes("ao");
    static final byte[] COLUMN_ASK_EXCHANGE = Bytes.toBytes("ae");

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");
    // public static final byte[] PRICEDATA_FAMILY_NAME = Bytes.toBytes("pf");

    private HTable wrfDataTable;

    private static final String WRF_TABLE_NAME = "WinROSFlowRecord";

    private boolean flushCommits = true;

    private volatile Configuration conf;

    private CountDownLatch countDownLatch = null;
    
    private FeedPlayback playback = null;

    public WRFDataWriter(String flowrecords) {
        super();
        playback = new FeedPlayback(flowrecords);
    }

    public void run() {
        try {
            initTables();
            wrfDataWrite();
            testTakedown();
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            if (this.countDownLatch != null) {
                this.countDownLatch.countDown();
            }
        }
    }

    private void wrfDataWrite() throws IOException {
    	WinROSFlowRecord record = null;
    	playback.open();
    	long total = 0;
    	
		List<Put> putList = new LinkedList<Put>();
        long putHeapSize = 0;
		
        long startTime = System.currentTimeMillis();
		while ((record = playback.next()) != null) {
			total++;  //totally we have 31843557
    	
            byte[] key = Bytes.toBytes(total);
			Put put = new Put(key);
			
            put.add(FAMILY_NAME, COLUMN_DWPRESIGNATURE, Bytes.toBytes(record.dwPostSignature));
            put.add(FAMILY_NAME, COLUMN_RECORDTYPE, Bytes.toBytes(record.RecordType));
            put.add(FAMILY_NAME, COLUMN_UNUSED, Bytes.toBytes(record.Unused));
            put.add(FAMILY_NAME, COLUMN_RECORDLENGTH, Bytes.toBytes(record.RecordLength));
            put.add(FAMILY_NAME, COLUMN_SEQUENCE, Bytes.toBytes(record.Sequence));
            put.add(FAMILY_NAME, COLUMN_SYMBOL, Bytes.toBytes(record.Symbol.trim()));
            put.add(FAMILY_NAME, COLUMN_DWPOSTSIGNATURE, Bytes.toBytes(record.dwPostSignature));
            put.add(FAMILY_NAME, COLUMN_FLAGS, Bytes.toBytes(record.TickData.Flags));
            put.add(FAMILY_NAME, COLUMN_SEQUENCESERIES,
                    Bytes.toBytes(record.TickData.SequenceSeries));
            put.add(FAMILY_NAME, COLUMN_CATEGORY, Bytes.toBytes(record.TickData.Category));
            put.add(FAMILY_NAME, COLUMN_SUBCATEGORY, Bytes.toBytes(record.TickData.SubCategory));
            put.add(FAMILY_NAME, COLUMN_LINEID, Bytes.toBytes(record.TickData.LineID));
            put.add(FAMILY_NAME, COLUMN_AUTHCODE, Bytes.toBytes(record.TickData.AuthCode));

            put.add(FAMILY_NAME, COLUMN_EXCHANGETIME, Bytes.toBytes(record.TickData.ExchangeTime));
            put.add(FAMILY_NAME, COLUMN_BEACON, Bytes.toBytes(record.TickData.Beacon));
            put.add(FAMILY_NAME, COLUMN_VWAP, Bytes.toBytes(record.TickData.VWap));
            put.add(FAMILY_NAME, COLUMN_SECQUALIFIERS, record.TickData.SecQualifiers);


            put.add(FAMILY_NAME, COLUMN_TRADE_VALUE,
                    Bytes.toBytes(record.TickData.Trade.value));
            put.add(FAMILY_NAME, COLUMN_TRADE_SIZE,
                    Bytes.toBytes(record.TickData.Trade.size));
            put.add(FAMILY_NAME, COLUMN_TRADE_UCUMVOLUME,
                    Bytes.toBytes(record.TickData.Trade.ucumvolume));
            put.add(FAMILY_NAME, COLUMN_TRADE_FLAGS,
                    Bytes.toBytes(record.TickData.Trade.flags));
            put.add(FAMILY_NAME, COLUMN_TRADE_QUALIFIERS,
                    record.TickData.Trade.qualifiers);
            put.add(FAMILY_NAME, COLUMN_TRADE_VOLQUALIFIERS,
                    record.TickData.Trade.volqualifiers);
            put.add(FAMILY_NAME, COLUMN_TRADE_EXCHANGE, record.TickData.Trade.exchange);

            put.add(FAMILY_NAME, COLUMN_BID_VALUE,
                    Bytes.toBytes(record.TickData.Bid.value));
            put.add(FAMILY_NAME, COLUMN_BID_SIZE, Bytes.toBytes(record.TickData.Bid.size));
            put.add(FAMILY_NAME, COLUMN_BID_UCUMVOLUME,
                    Bytes.toBytes(record.TickData.Bid.ucumvolume));
            put.add(FAMILY_NAME, COLUMN_BID_FLAGS,
                    Bytes.toBytes(record.TickData.Bid.flags));
            put.add(FAMILY_NAME, COLUMN_BID_QUALIFIERS, record.TickData.Bid.qualifiers);
            put.add(FAMILY_NAME, COLUMN_BID_VOLQUALIFIERS,
                    record.TickData.Bid.volqualifiers);
            put.add(FAMILY_NAME, COLUMN_BID_EXCHANGE, record.TickData.Bid.exchange);

            put.add(FAMILY_NAME, COLUMN_ASK_VALUE,
                    Bytes.toBytes(record.TickData.Ask.value));
            put.add(FAMILY_NAME, COLUMN_ASK_SIZE, Bytes.toBytes(record.TickData.Ask.size));
            put.add(FAMILY_NAME, COLUMN_ASK_UCUMVOLUME,
                    Bytes.toBytes(record.TickData.Ask.ucumvolume));
            put.add(FAMILY_NAME, COLUMN_ASK_FLAGS,
                    Bytes.toBytes(record.TickData.Ask.flags));
            put.add(FAMILY_NAME, COLUMN_ASK_QUALIFIERS, record.TickData.Ask.qualifiers);
            put.add(FAMILY_NAME, COLUMN_ASK_VOLQUALIFIERS,
                    record.TickData.Ask.volqualifiers);
            put.add(FAMILY_NAME, COLUMN_ASK_EXCHANGE, record.TickData.Ask.exchange);

            put.setWriteToWAL(false);

            putHeapSize += put.heapSize();
			putList.add(put);
			
            if (total % 500 == 0) {
                this.wrfDataTable.put(putList);
                putList = new LinkedList<Put>();

                this.wrfDataTable.flushCommits();
                // putHeapSize = 0 ;
            }
            // if (total == 1843557) {
            // break;
            // }
		}
		
		if (putList.size() > 0) {
            this.wrfDataTable.put(putList);
        }
        long spendTime = System.currentTimeMillis() - startTime;
        // The file size is 5810 MB
        // double performance = 5810d / (double)spendTime * 1000;
        double performance = ((double) putHeapSize / 1024d / 1024d) / (double) spendTime * 1000;
        
        System.out.println("Writing performance is : " + performance + " MB per second");

    }

    private void initTables() throws IOException {
        this.wrfDataTable = new HTable(conf, WRF_TABLE_NAME);
        //this.wrfDataTable.setWriteBufferSize(HTABLE_BUFFER_SIZE);
        this.wrfDataTable.setAutoFlush(false);
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    private void testTakedown() throws IOException {
        if (flushCommits) {
            this.wrfDataTable.flushCommits();
        }
        this.wrfDataTable.close();
    }

}


