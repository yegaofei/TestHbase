package com.insigmaus.tic;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 27, 2013
 */

public class TicDataReader implements Runnable {

    static final byte[] COLUMN_TIME = Bytes.toBytes("t");
    static final byte[] COLUMN_FLAGS = Bytes.toBytes("f");
    static final byte[] COLUMN_EXH_TIME = Bytes.toBytes("e");
    static final byte[] COLUMN_SEQUENCENUMBER = Bytes.toBytes("s");
    static final byte[] COLUMN_LINEID = Bytes.toBytes("l");
    static final byte[] COLUMN_SEQUENCESEIRES = Bytes.toBytes("ss");
    static final byte[] COLUMN_SEQUENCEQAULIFIER = Bytes.toBytes("sq");
    static final byte[] COLUMN_TRADEEXCHANGE = Bytes.toBytes("te");
    static final byte[] COLUMN_TRADEPRICE = Bytes.toBytes("p");
    static final byte[] COLUMN_VWAP = Bytes.toBytes("v");
    static final byte[] COLUMN_TRADEVOLUME = Bytes.toBytes("tv");
    static final byte[] COLUMN_QUALIFIERS = Bytes.toBytes("q");
    static final byte[] COLUMN_CUMVOLUME = Bytes.toBytes("c");
    static final byte[] COLUMN_VOLQUALIFIERS = Bytes.toBytes("vq");
    static final byte[] COLUMN_ASK_EXCHANGE = Bytes.toBytes("a");
    static final byte[] COLUMN_BID_EXCHANGE = Bytes.toBytes("b");
    static final byte[] COLUMN_BID_PRICE = Bytes.toBytes("bp");
    static final byte[] COLUMN_ASK_PRICE = Bytes.toBytes("ap");
    static final byte[] COLUMN_BID_SIZE = Bytes.toBytes("bz");
    static final byte[] COLUMN_ASK_SIZE = Bytes.toBytes("az");

    private static final int SCAN_CACHE_LINES = 1000;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private int startIndex;

    private int endIndex;

    private HTable ticTradetable;

    private HTable ticQuoteTable;

    private static final String TIC_TRADE_TABLE_NAME = "Tic_Trade";

    private static final String TIC_QUOTE_TABLE_NAME = "Tic_Quote";

    private boolean flushCommits = true;

    private volatile Configuration conf;

    private CountDownLatch countDownLatch = null;

    private SymbolCount[] symbolCountArray = null;

    private List<String> symbolList = null;

    private int fetchRecodeCount = 0;

    private SymbolDataTool symbolDataTool = null;

    public TicDataReader(Configuration conf, List<String> symbolList) {
        this.conf = conf;
        this.symbolList = symbolList;
    }

    public void run() {
        try {
            this.symbolDataTool = new SymbolDataTool(this.conf);
            initTables();
            scanTable();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if (this.countDownLatch != null) {
                this.countDownLatch.countDown();
            }
        }
    }

    private void scanTable() throws IOException {
        if (symbolList == null) {
            return;
        }

        double performance = 0d;
        long timeSpend = 0L;
        for (String symbol : symbolList) {
            // System.out.println("Processing symbol: " + symbol);
            SymbolData sd = symbolDataTool.searchSymbolData(symbol);
            if (sd == null) {
                continue;
            }

            int baseKey = RowKeyGenerator.generateRowKey(sd.getStartKey(), 0);

            Scan scan = new Scan(Bytes.toBytes(baseKey), Bytes.toBytes(sd.getEndKey()));
                scan.addFamily(FAMILY_NAME);
                scan.setCaching(SCAN_CACHE_LINES);

                long startTime = System.currentTimeMillis();
                ResultScanner scanner = this.ticQuoteTable.getScanner(scan);
                Iterator<Result> res = scanner.iterator();
                while (res.hasNext()) {
                    res.next();
                    fetchRecodeCount++;
                }
                timeSpend = timeSpend + (System.currentTimeMillis() - startTime);

                startTime = System.currentTimeMillis();
                scanner = this.ticTradetable.getScanner(scan);
                res = scanner.iterator();
                while (res.hasNext()) {
                    res.next();
                    fetchRecodeCount++;
                }
                timeSpend = timeSpend + (System.currentTimeMillis() - startTime);

        }
        performance = (double) fetchRecodeCount / (double) timeSpend * 1000;
        System.out.println(" Fetch " + fetchRecodeCount + " rows for Symbol list: ["
                + printSymbolList(symbolList)
 + "\b], performance is " + performance + " r/s");
    }

    private String printSymbolList(List<String> symbolList) {
        StringBuilder sb = new StringBuilder();
        for (String s : symbolList) {
            sb.append(s).append(",");
        }
        return sb.toString();
    }

    private void initTables() throws IOException {
        this.ticTradetable = new HTable(conf, TIC_TRADE_TABLE_NAME);
        this.ticTradetable.setAutoFlush(false);

        this.ticQuoteTable = new HTable(conf, TIC_QUOTE_TABLE_NAME);
        this.ticQuoteTable.setAutoFlush(false);
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(int endIndex) {
        this.endIndex = endIndex;
    }

    public HTable getTicTradetable() {
        return ticTradetable;
    }

    public void setTicTradetable(HTable ticTradetable) {
        this.ticTradetable = ticTradetable;
    }

    public HTable getTicQuoteTable() {
        return ticQuoteTable;
    }

    public void setTicQuoteTable(HTable ticQuoteTable) {
        this.ticQuoteTable = ticQuoteTable;
    }

    public boolean isFlushCommits() {
        return flushCommits;
    }

    public void setFlushCommits(boolean flushCommits) {
        this.flushCommits = flushCommits;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    public SymbolCount[] getSymbolCountArray() {
        return symbolCountArray;
    }

    public void setSymbolCountArray(SymbolCount[] symbolCountArray) {
        this.symbolCountArray = symbolCountArray;
    }

}


