/**
 * 
 */
package com.insigma.tickserver;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Java FeedPlayback
 * 
 * @author gpan
 * 
 */
public class FeedPlayback implements Closeable {
	public static int PAGE_SIZE = 4096;
	
	public FeedPlayback(String file) {
		replayFile = file;
		inputStream = null;
		totalRecordsRead = 0;
		batchRecordsIndex = 0;
		totalBytesRead = 0;
	}
	
	/**
	 * Open stream for reading 
	 * 
	 * @throws IOException
	 */
	public void open() throws IOException {
		closeStream();
		
		fis = new FileInputStream(replayFile);
		inputStream = new DataInputStream(new BufferedInputStream(fis));
	}
	
	/**
	 * Close all open streams and reset 
	 * 
	 * @throws IOException
	 */
	private void closeStream() throws IOException {
		if (fis != null) {
			fis.close();
		}
		
		if (inputStream != null) {
			inputStream.close();		
		}
		
		totalRecordsRead = 0;
		batchRecordsIndex = 0;
		totalBytesRead = 0;
	}
	
	/**
	 * Read in next batch of records into cache
	 * 
	 * @return true for success, false for EOF
	 * @throws IOException
	 */
	private boolean readNextBatchFromFile() throws IOException {
		if (inputStream.available() == 0) {
			return false;
		}
				
		inputStream.readFully(dwTime);	
		totalBytesRead += dwTime.length;
		
		inputStream.readFully(nRecs);
		totalBytesRead += nRecs.length;		
		
		int numRecs = nRecs[0];

		byte[] rawRecord = new byte[WinROSFlowRecord.RECORD_SIZE];

//		System.out.println(Bytes.toShort(dwTime));
        // System.out.println(nRecs[0]);

		batchRecords = new WinROSFlowRecord[numRecs];
		batchRecordsIndex = 0;

		for (int i = 0; i < numRecs; ++i) {
			inputStream.readFully(rawRecord);
			batchRecords[i] = new WinROSFlowRecord();
			batchRecords[i].fromBytes(rawRecord);
			totalRecordsRead++;
			totalBytesRead += rawRecord.length;
			
            /*
             * System.out.println(batchRecords[i].dwPreSignature);
             * System.out.println(batchRecords[i].TickData.ExchangeTime);
             * System.out.println(batchRecords[i].RecordType);
             * System.out.println(batchRecords[i].Symbol);
             * System.out.println(batchRecords[i].TickData.VWap);
             * System.out.println(new
             * String(batchRecords[i].TickData.Bid.exchange));
             * System.out.println(batchRecords[i].TickData.Bid.value);
             * System.out.println(batchRecords[i].TickData.Bid.size);
             * System.out.println(new
             * String(batchRecords[i].TickData.Ask.exchange));
             * System.out.println(batchRecords[i].TickData.Ask.value);
             * System.out.println(batchRecords[i].TickData.Ask.size);
             * System.out.println(new
             * String(batchRecords[i].TickData.Trade.exchange));
             * System.out.println(batchRecords[i].TickData.Trade.value);
             * System.out.println(batchRecords[i].TickData.Trade.size);
             * 
             * System.out.println(batchRecords[i].dwPostSignature);
             * System.out.println("-----------------------------------");
             */
		}
		

		return true;
	}
	
	/**
	 * Get total number of records read from file
	 * 
	 * @return
	 */
	public long getTotalRecordsRead() {
		return totalRecordsRead;
	}

	
	/**
	 * Get next record for replay
	 * 
	 * @return instance of record or null for EOF
	 * @throws IOException
	 */
	public WinROSFlowRecord next() throws IOException {
		if (batchRecords != null && batchRecordsIndex < batchRecords.length) {
			return batchRecords[batchRecordsIndex++];
		}
		
		if (readNextBatchFromFile()) {
			return next();
		}
		
		return null;
	}
	

	
	public void close() throws IOException {
		closeStream();
	}
	
	private String replayFile;
	private DataInputStream inputStream;
	private long totalRecordsRead;
	private long totalBytesRead;
	
	public long getTotalBytesRead() {
		return totalBytesRead;
	}

	public void setTotalBytesRead(long totalBytesRead) {
		this.totalBytesRead = totalBytesRead;
	}

	public void setTotalRecordsRead(long totalRecordsRead) {
		this.totalRecordsRead = totalRecordsRead;
	}

	private byte[] dwTime = new byte[4];
	private byte[] nRecs = new byte[1];
	private WinROSFlowRecord[] batchRecords;
	private int batchRecordsIndex;
	private FileInputStream fis;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please input the flowrecords file path and name");
            System.exit(-1);
        }

		try {

            FeedPlayback playback = new FeedPlayback(args[0]);
			WinROSFlowRecord record = null;

			long total = 0;

			
			playback.open();
			
			long startTime = System.currentTimeMillis();
			while ((record = playback.next()) != null) {
				total++;
				
				if (total % 1000000 == 0) {
					System.out.println(System.currentTimeMillis() + " " + total + " records read");
				}
			}
			long overallSpendTime = System.currentTimeMillis()  - startTime;
			
			System.out.println("Total bytes read : " + playback.getTotalBytesRead() + " bytes");
			System.out.println("Total records read : " + playback.getTotalRecordsRead());
			double performance = ((double)playback.getTotalBytesRead() /(1024d * 1024d)) / (double)overallSpendTime * 1000;
			System.out.println("Overall performance : " + performance + " MB per second");
			
			playback.close();

			System.out.println("In total " + total + " records read");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
