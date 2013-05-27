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

public class TicQuoteDataWriter implements TicDataWriter {

    private int startIndex;

    private int endIndex;

    private HTable ticQuoteTable;

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

    public TicQuoteDataWriter(int startIndex, int endIndex) {
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
            TicQuoteCID[] ticQuote = this.tdg.generateTicQuoteCIDArray(symbol, count - tradeCount);
            List<Put> putListQuote = new LinkedList<Put>();
            for (int k = 0; k < ticQuote.length; k++) {
                TicQuoteCID quote = ticQuote[k];
                byte[] key = Bytes.toBytes(RowKeyGenerator.generateRowKey(startKey, k));
                Put put = new Put(key);
                put.add(FAMILY_NAME, COLUMN_TIME, Bytes.toBytes(quote.gettTime()));
                put.add(FAMILY_NAME, COLUMN_FLAGS, Bytes.toBytes(quote.getuFlags()));
                put.add(FAMILY_NAME, COLUMN_EXH_TIME, Bytes.toBytes(quote.getExchangeTime()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCENUMBER,
                        Bytes.toBytes(quote.getSequenceNumber()));
                put.add(FAMILY_NAME, COLUMN_LINEID, Bytes.toBytes(quote.getLineID()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCESEIRES,
                        Bytes.toBytes(quote.getSequenceSeries()));
                put.add(FAMILY_NAME, COLUMN_SEQUENCEQAULIFIER, quote.getSecqualifiers());
                put.add(FAMILY_NAME, COLUMN_ASK_EXCHANGE,
                        new String(quote.getcAskExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_BID_EXCHANGE,
                        new String(quote.getcBidExchange()).getBytes());
                put.add(FAMILY_NAME, COLUMN_BID_PRICE, Bytes.toBytes(quote.getdBidPrice()));
                put.add(FAMILY_NAME, COLUMN_ASK_PRICE, Bytes.toBytes(quote.getdAskPrice()));
                put.add(FAMILY_NAME, COLUMN_BID_SIZE, Bytes.toBytes(quote.getiBidSize()));
                put.add(FAMILY_NAME, COLUMN_ASK_SIZE, Bytes.toBytes(quote.getiAskSize()));
                putListQuote.add(put);

                if ((k % 500 == 0) && (k > 0)) {
                    this.ticQuoteTable.put(putListQuote);
                    putListQuote = new LinkedList<Put>();
                }
            }

            // long tStart = System.currentTimeMillis();
            if (putListQuote.size() > 0) {
                this.ticQuoteTable.put(putListQuote);
            }

            insertedRows += count;

            if (insertedRows > 1000000) {
                long elaspTime = System.currentTimeMillis() - startTime;
                System.out.print(Thread.currentThread().getName() + "-Quote, "
                        + System.currentTimeMillis() + ", ");
                double performance = (double) insertedRows / (double) elaspTime * 1000;
                System.out.println(performance);

                insertedRows = 0;
                startTime = System.currentTimeMillis();
            }
        }
    }

    private void initTables() throws IOException {
        this.ticQuoteTable = new HTable(conf, TIC_QUOTE_TABLE_NAME);
        // this.ticQuoteTable.setWriteBufferSize(HTABLE_BUFFER_SIZE);
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
            this.ticQuoteTable.flushCommits();

        }
        this.ticQuoteTable.close();
    }
}


