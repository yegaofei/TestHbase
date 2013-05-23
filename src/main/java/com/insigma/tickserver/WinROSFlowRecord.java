package com.insigma.tickserver;

import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * FLOWRECORD_WINROS
 * 
 * @author gpan
 *
 */
public class WinROSFlowRecord {
	public static int RECORD_SIZE = 188;
	
	public long	dwPreSignature;	/* sync verification -- FR_PRESIG -- */
	public byte	RecordType;		/* Identifies following record type */
	public byte	Unused;			/* for ??? in future */
	public int	RecordLength;	/* Total length of data rec in case different recs in future */
	public long	Sequence;		/* Counter for verifying buffer reliability */
	public String Symbol;		/* Symbol for record */
	public TickDataType	TickData;
	public long	dwPostSignature;	/* sync verification -- FR_POSTSIG -- */	
	
	public byte[] rawData;
	
	public WinROSFlowRecord() {
		dwPreSignature = Sequence = dwPostSignature = 0;
		RecordLength = 0;
		//Symbol = new char[48];
		TickData = new TickDataType();
	}
	
	/**
	 * Convert from binary record to structure
	 * @param raw
	 * @throws IllegalArgumentException
	 */
	public void fromBytes(byte []raw) throws IllegalArgumentException {
		if (raw == null || raw.length < RECORD_SIZE) {
			throw new java.lang.IllegalArgumentException("Insufficient raw data");
		}
		
		this.rawData = raw;
		
		int offset = 0;	
		
		//public long	dwPreSignature;	/* sync verification -- FR_PRESIG -- */
		offset += 4;
		//public byte	RecordType;		/* Identifies following record type */
		offset += 1;
		//public byte	Unused;			/* for ??? in future */
		offset += 1;
		//public int	RecordLength;	/* Total length of data rec in case different recs in future */
		offset += 2;
		// public long	Sequence;		/* Counter for verifying buffer reliability */
		offset += 4;
		//public char	[]Symbol;		/* Symbol for record */
		Symbol = Bytes.toString(Arrays.copyOfRange(raw, offset, offset + 48));
		offset += 48;
		// public TickDataType	TickData;
		TickData.fromBytes(Arrays.copyOfRange(raw, offset, offset + TickDataType.RECORD_SIZE + 1));
		offset += TickDataType.RECORD_SIZE;
		//public long	dwPostSignature;	/* sync verification -- FR_POSTSIG -- */	
		offset += 4;
	}
}
