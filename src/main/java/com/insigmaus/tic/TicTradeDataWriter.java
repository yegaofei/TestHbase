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
 * @version V1.0  Create Time: May 7, 2013
 */

public class TicTradeDataWriter implements TicDataWriter {

    private int startIndex;

    private int endIndex;

    private HTable ticTradetable;

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

    public TicTradeDataWriter(int startIndex, int endIndex) {
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
        long startTime = System.currentTimeMillis();
        int insertedRows = 0;
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

            List<Put> putList = new LinkedList<Put>();
            for (int k = 0; k < ticTrade.length; k++) {
                TicTradeCID trade = ticTrade[k];
                byte[] key = Bytes.toBytes(RowKeyGenerator.generateRowKey(startKey, k));
                Put put = new Put(key);
                put.add(FAMILY_NAME, COLUMN_TIME, Bytes.toBytes(trade.gettTime()));
                put.add(FAMILY_NAME, COLUMN_FLAGS, Bytes.toBytes(trade.getuFlags()));
                put.add(FAMILY_NAME, COLUMN_EXH_TIME, Bytes.toBytes(trade.getExchangeTime()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCENUMBER,
                        Bytes.toBytes(trade.getSequenceNumber()));
                put.add(FAMILY_NAME, COLUMN_LINEID, Bytes.toBytes(trade.getLineID()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCESEIRES,
                        Bytes.toBytes(trade.getSequenceSeries()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCEQAULIFIER, trade.getSecqualifiers());
                put.add(FAMILY_NAME, COLUMN_TRADEEXCHANGE,
                        new String(trade.getcTradeExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_TRADEPRICE, Bytes.toBytes(trade.getdTradePrice()));
                put.add(FAMILY_NAME, COLUMN_VWAP, Bytes.toBytes(trade.getdVWAP()));
                put.add(FAMILY_NAME, COLUMN_TRADEVOLUME, Bytes.toBytes(trade.getiTradeVolume()));
                put.add(FAMILY_NAME, COLUMN_QUALIFIERS, trade.getQualifiers());
                put.add(FAMILY_NAME, COLUMN_CUMVOLUME, Bytes.toBytes(trade.getuCumVolume()));
                put.add(FAMILY_NAME, COLUMN_VOLQUALIFIERS, trade.getVolqualifiers());
                putList.add(put);

                if ((k % 1000 == 0) && (k > 0)) {
                    this.ticTradetable.put(putList);
                    putList = new LinkedList<Put>();
                }
            }

            if (putList.size() > 0) {
                this.ticTradetable.put(putList);
            }

            insertedRows += tradeCount;

            if (insertedRows > 1000000) {
                long elaspTime = System.currentTimeMillis() - startTime;
                System.out.print(Thread.currentThread().getName() + "-Trade, "
                        + System.currentTimeMillis() + ", ");
                double performance = (double) insertedRows / (double) elaspTime * 1000;
                System.out.println(performance);
                insertedRows = 0;
                startTime = System.currentTimeMillis();
            }
        }
    }

    private void initTables() throws IOException {
        this.ticTradetable = new HTable(conf, TIC_TRADE_TABLE_NAME);
        // this.ticTradetable.setWriteBufferSize(HTABLE_BUFFER_SIZE);
        this.ticTradetable.setAutoFlush(false);
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

        }
        this.ticTradetable.close();
    }
}


