package com.insigmaus.tic;

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

public class TicDataWriter implements Runnable {

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

    private static final int HTABLE_BUFFER_SIZE = 1024 * 1024 * 20;

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private int startIndex;

    private int endIndex;

    private HTable ticTradetable;

    private HTable ticQuoteTable;

    private static final String TIC_TRADE_TABLE_NAME = "Tic_Trade";

    private static final String TIC_QUOTE_TABLE_NAME = "Tic_Quote";

    private SymbolDataTool symbolDataTool = null;

    private boolean flushCommits = true;

    private volatile Configuration conf;

    private CountDownLatch countDownLatch = null;

    private SymbolCount[] symbolCountArray = null;

    public SymbolCount[] getSymbolCountArray() {
        return symbolCountArray;
    }

    public void setSymbolCountArray(SymbolCount[] symbolCountArray) {
        this.symbolCountArray = symbolCountArray;
    }

    private TicDataGenerate tdg = null;

    public TicDataWriter(int startIndex, int endIndex) {
        super();
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.tdg = new TicDataGenerate();
    }

    public void run() {
        if (symbolCountArray == null) {
            return;
        }
        this.symbolDataTool = new SymbolDataTool(this.conf);
        try {
            initTables();
            ticDataWrite();
            testTakedown();
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            if (this.countDownLatch != null) {
                this.countDownLatch.countDown();
            }
        }
    }

    private void ticDataWrite() throws IOException {
        for (int i = this.startIndex; i < this.endIndex; i++) {
            SymbolCount symbolCount = symbolCountArray[i];
            String symbol = symbolCount.getSymbol();
            int count = symbolCount.getCount();

            SymbolData sd = symbolDataTool.searchSymbolData(symbol);
            int startKey = sd.getStartKey();
            int endKey = sd.getEndKey();

            if (endKey != (startKey + count)) {
                // Verify if the start key and end key are correct
                throw new RuntimeException("The start key and end key for symbol " + symbol
                        + " is wrong!");
            }

            int tradeCount = Math.round(count / 10);
            TicTradeCID[] ticTrade = this.tdg.generateTicTradeCID(symbol, tradeCount);

            long putHeapSize = 0 ;
            List<Put> putList = new LinkedList<Put>();
            for (int k = 0; k < ticTrade.length; k++) {
                TicTradeCID trade = ticTrade[k];
                byte[] key = Bytes.toBytes(RowKeyGenerator.generateRowKey(startKey, k));
                Put put = new Put(key);
                put.add(FAMILY_NAME, COLUMN_TIME, Bytes.toBytes(trade.gettTime()));
                put.add(FAMILY_NAME, COLUMN_FLAGS,
                        Bytes.toBytes(trade.getuFlags()));
                put.add(FAMILY_NAME, COLUMN_EXH_TIME,
                        Bytes.toBytes(trade.getExchangeTime()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCENUMBER,
                        Bytes.toBytes(trade.getSequenceNumber()));
                put.add(FAMILY_NAME, COLUMN_LINEID,
                        Bytes.toBytes(trade.getLineID()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCESEIRES,
                        Bytes.toBytes(trade.getSequenceSeries()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCEQAULIFIER,
                        trade.getSecqualifiers());
                put.add(FAMILY_NAME, COLUMN_TRADEEXCHANGE,
                        new String(trade.getcTradeExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_TRADEPRICE,
                        Bytes.toBytes(trade.getdTradePrice()));
                put.add(FAMILY_NAME, COLUMN_VWAP, Bytes.toBytes(trade.getdVWAP()));
                put.add(FAMILY_NAME, COLUMN_TRADEVOLUME,
                        Bytes.toBytes(trade.getiTradeVolume()));
                put.add(FAMILY_NAME, COLUMN_QUALIFIERS, trade.getQualifiers());
                put.add(FAMILY_NAME, COLUMN_CUMVOLUME,
                        Bytes.toBytes(trade.getuCumVolume()));
                put.add(FAMILY_NAME, COLUMN_VOLQUALIFIERS,
                        trade.getVolqualifiers());
                putHeapSize += put.heapSize();
                putList.add(put);

                if ((k % 1000 == 0) && (k > 0)) {
                    this.ticTradetable.put(putList);
                    putList = new LinkedList<Put>();
                    putHeapSize = 0 ;
                }
            }

            if (putList.size() > 0) {
                this.ticTradetable.put(putList);
            }

            TicQuoteCID[] ticQuote = this.tdg.generateTicQuoteCIDArray(symbol, count - tradeCount);
            List<Put> putListQuote = new LinkedList<Put>();
            for (int k = 0; k < ticQuote.length; k++) {
                TicQuoteCID quote = ticQuote[k];
                byte[] key = Bytes.toBytes(RowKeyGenerator.generateRowKey(startKey, k));
                Put put = new Put(key);
                put.add(FAMILY_NAME, COLUMN_TIME, Bytes.toBytes(quote.gettTime()));
                put.add(FAMILY_NAME, COLUMN_FLAGS,
                        Bytes.toBytes(quote.getuFlags()));
                put.add(FAMILY_NAME, COLUMN_EXH_TIME,
                        Bytes.toBytes(quote.getExchangeTime()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCENUMBER,
                        Bytes.toBytes(quote.getSequenceNumber()));
                put.add(FAMILY_NAME, COLUMN_LINEID,
                        Bytes.toBytes(quote.getLineID()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCESEIRES,
                        Bytes.toBytes(quote.getSequenceSeries()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCEQAULIFIER,
                        quote.getSecqualifiers());
                put.add(FAMILY_NAME, COLUMN_ASK_EXCHANGE,
                        new String(quote.getcAskExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_BID_EXCHANGE,
                        new String(quote.getcBidExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_BID_PRICE,
                        Bytes.toBytes(quote.getdBidPrice()));
                put.add(FAMILY_NAME, COLUMN_ASK_PRICE,
                        Bytes.toBytes(quote.getdAskPrice()));
                put.add(FAMILY_NAME, COLUMN_BID_SIZE,
                        Bytes.toBytes(quote.getiBidSize()));
                put.add(FAMILY_NAME, COLUMN_ASK_SIZE,
                        Bytes.toBytes(quote.getiAskSize()));
                putListQuote.add(put);

                if ((k % 1000 == 0) && (k > 0)) {
                        this.ticQuoteTable.put(putListQuote);
                        putListQuote = new LinkedList<Put>();
                }
            }

            // long tStart = System.currentTimeMillis();
            if (putListQuote.size() > 0) {
                this.ticQuoteTable.put(putListQuote);
            } 
            // long spendTime = System.currentTimeMillis() - tStart;
            // if (spendTime > 0) {
            // // System.out.print("insert " + putListQuote.size() +
            // // " TicQuoteCIDs by spending "
            // // + spendTime);
            // // double performance = (double) putListQuote.size() / (double)
            // spendTime * 1000;
            // // System.out.println(",    Performance is " + performance +
            // // " r/s");
            // //System.out.println(performance);
            // }
        }
    }

    private void initTables() throws IOException {
        this.ticTradetable = new HTable(conf, TIC_TRADE_TABLE_NAME);
        this.ticTradetable.setWriteBufferSize(HTABLE_BUFFER_SIZE);
        this.ticTradetable.setAutoFlush(false);

        this.ticQuoteTable = new HTable(conf, TIC_QUOTE_TABLE_NAME);
        this.ticQuoteTable.setWriteBufferSize(HTABLE_BUFFER_SIZE);
        this.ticQuoteTable.setAutoFlush(false);
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
            this.ticTradetable.flushCommits();
            this.ticQuoteTable.flushCommits();

        }
        this.ticTradetable.close();
        this.ticQuoteTable.close();
    }

}


