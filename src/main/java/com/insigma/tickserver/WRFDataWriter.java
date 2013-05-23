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

    static final byte[] COLUMN_WRF = Bytes.toBytes("t");

    public static final byte[] FAMILY_NAME = Bytes.toBytes("cf");

    private HTable wrfDataTable;

    private static final String WRF_TABLE_NAME = "WinROSFlowRecord";

    private boolean flushCommits = true;

    private volatile Configuration conf;

    private CountDownLatch countDownLatch = null;
    
    private FeedPlayback playback = new FeedPlayback("/Users/phillip/Downloads/flowrecords.bin");

    public WRFDataWriter() {
        super();
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
    	
		//long startTime = System.currentTimeMillis();
		List<Put> putList = new LinkedList<Put>();
		long putHeapSize = 0 ;
		
		while ((record = playback.next()) != null) {
			total++;
    	
			byte[] key = Bytes.toBytes(record.Symbol);
			Put put = new Put(key);
			
			put.add(FAMILY_NAME, COLUMN_WRF, record.rawData );
			putHeapSize += put.heapSize();
			putList.add(put);
			
			if ((total % 1000 == 0) && (total > 0)) {
                this.wrfDataTable.put(putList);
                putList = new LinkedList<Put>();
                putHeapSize = 0 ;
            }
		}
		
		if (putList.size() > 0) {
            this.wrfDataTable.put(putList);
        }
		
		
		
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


