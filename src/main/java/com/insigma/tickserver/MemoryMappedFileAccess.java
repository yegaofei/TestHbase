package com.insigma.tickserver;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MemoryMappedFileAccess implements Closeable {
	
	public static final int length = 0xFFFFFFF; // 256 Mb
	
	private long fcPosition = 0;
	
	private ByteBuffer cacheLine = null;
	
	private FileInputStream fs = null;
	
	private FileChannel fc = null;
	
	private WinROSFlowRecord[] batchRecords = new WinROSFlowRecord[0x0000FFF];
	{
		for(int i = 0 ; i < batchRecords.length; i++){
			batchRecords[i] = new WinROSFlowRecord();
		}
	}
	
	private long totalRecordsRead;
	
	public long getTotalRecordsRead() {
		return totalRecordsRead;
	}

	public void setTotalRecordsRead(long totalRecordsRead) {
		this.totalRecordsRead = totalRecordsRead;
	}

	public long getTotalBytesRead() {
		return totalBytesRead;
	}

	public void setTotalBytesRead(long totalBytesRead) {
		this.totalBytesRead = totalBytesRead;
	}

	private long totalBytesRead;
	
	private byte[] rawRecord = new byte[WinROSFlowRecord.RECORD_SIZE];
	
	public MemoryMappedFileAccess(String fileName) {
		super();
		this.fileName = fileName;
	}

	private String fileName;
	
	private byte[] dwTime = new byte[4];
	private byte[] nRecs = new byte[1];
	
	
	public void open() throws IOException {		
		fs = new FileInputStream(new File(fileName));
		fc = fs.getChannel();
		cacheLine = fc.map(FileChannel.MapMode.READ_ONLY, 0, length).asReadOnlyBuffer();	
	}
	
	private boolean readNextBatch() throws IOException{
		
		if(cacheLine.remaining() < (4 + 1)){
			fcPosition += (long)cacheLine.position() ;
									
			try{
				if(fcPosition + length < fc.size()){
					cacheLine = fc.map(FileChannel.MapMode.READ_ONLY, fcPosition, length).asReadOnlyBuffer();						
				}else{
					cacheLine = fc.map(FileChannel.MapMode.READ_ONLY, fcPosition, fc.size() - fcPosition).asReadOnlyBuffer();	
				}								
			}catch (IOException ioe){
				ioe.printStackTrace();				
			}
		}
		
		cacheLine.get(dwTime, 0, 4); //The position of this buffer is then incremented by length. 
		cacheLine.get(nRecs, 0, 1);
		
		int numRecs = nRecs[0];
		
		if(cacheLine.remaining() < WinROSFlowRecord.RECORD_SIZE * numRecs){
			fcPosition += (long)cacheLine.position();						
			
			try{
				if(fcPosition + length < fc.size()){
					cacheLine = fc.map(FileChannel.MapMode.READ_ONLY, fcPosition, length).asReadOnlyBuffer();	
				} else {
					cacheLine = fc.map(FileChannel.MapMode.READ_ONLY, fcPosition, fc.size() - fcPosition).asReadOnlyBuffer();	
				}
			}catch (IOException ioe){
				ioe.printStackTrace();	
			}
		}
		
		if(cacheLine.remaining() < WinROSFlowRecord.RECORD_SIZE * numRecs){
			throw new RuntimeException("Please increase the cache line size, it's too small to cache one batch of records");
		}

//		System.out.println(Bytes.toShort(dwTime));
//		System.out.println(nRecs[0]);
		
		for (int i = 0; i < numRecs; ++i) {
			cacheLine.get(rawRecord, 0, rawRecord.length);
			batchRecords[i].fromBytes(rawRecord);
			totalRecordsRead++;
			totalBytesRead += rawRecord.length;
		}	
		
		if((fcPosition + (long)cacheLine.position()) >= fc.size()){
			return false;
		}
		
		return true;
	}
	
	public void read() throws IOException{
		
		if(fs == null || fc == null){
			throw new RuntimeException("Please call open() before read()");
		}
		
		long total = 0;
		long startTime = System.currentTimeMillis();
		while(readNextBatch()){
			total++;
			
			if (total % 1000000 == 0) {
				long time = System.currentTimeMillis()  - startTime;
				double perf = 1000000d / (double)time * 1000;
				System.out.println("1000000 records read " + perf + "mps");
				startTime = System.currentTimeMillis();
			}
		}	
		
	}
	
	public void close() throws IOException {
		if(fc != null){
			fc.close();
		}
		
		if(fs != null){
			fs.close();
		}
		
	}
	
	public static void main(String[] args){
        MemoryMappedFileAccess mma = new MemoryMappedFileAccess("E:\\flowrecords\\flowrecords.bin");
		
		try {
			mma.open();
			long startTime = System.currentTimeMillis();
			mma.read();
			long spendTime = System.currentTimeMillis()- startTime;
			System.out.println("Total bytes read : " + mma.getTotalBytesRead() + " bytes");
			System.out.println("Total records read : " + mma.getTotalRecordsRead());
			double performance = ((double)mma.getTotalBytesRead() /(1024d * 1024d)) / (double)spendTime * 1000;
			System.out.println("Overall performance : " + performance + " MB per second");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				mma.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}

}
