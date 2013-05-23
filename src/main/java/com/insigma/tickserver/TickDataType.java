package com.insigma.tickserver;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * TICKDATA
 * 
 * @author gpan
 *
 */
public class TickDataType {
	public int	Flags;	// indicates whether price and bid/ask have been split by winrosmt, trade or bidask data rec, unknown items
	public byte	SequenceSeries;	/* 3 fields encoded here...passed from fnet */
	public byte	Category;
	public byte	SubCategory;	/* is this needed? */
	public int	LineID;	/* identifies line incoming data came in on (used for sequence numbers sets) */
	public int	AuthCode;	/* authcode */
	public long	ExchangeTime;	/* TIME_T representing time of transaction or transactions */
	public long	Beacon;		/* beacon time, used for ticordering based upon beacon */
	public double	VWap;		/* vwap */
	public long	SequenceNumber;	/* sequence number for LineID */
	public byte	[]SecQualifiers;
	public PriceDataType	Trade;
	public PriceDataType	Bid;
	public PriceDataType	Ask;
	
	public static int TICKDATA_SECQUAL_SIZE = 4;
	
	public static int RECORD_SIZE = 124;
	
	public TickDataType() {
		SecQualifiers = new byte[TICKDATA_SECQUAL_SIZE];
		Flags = LineID = AuthCode = 0;
		ExchangeTime = Beacon = 0;
		VWap = 0.0;
		Trade = new PriceDataType();
		Bid = new PriceDataType();
		Ask = new PriceDataType();
	}
	
	/**
	 * Convert from binary record to structure
	 * @param raw
	 * @throws IllegalArgumentException
	 */
	public void fromBytes(byte []raw) throws IllegalArgumentException {
		if (raw == null || raw.length < RECORD_SIZE) {
			throw new java.lang.IllegalArgumentException("Insufficient raw data. Got " + raw.length + " Expecting " + RECORD_SIZE);
		}
		
		int offset = 0;
		//WORD	Flags;	// indicates whether price and bid/ask have been split by winrosmt, trade or bidask data rec, unknown items
		Flags = Bytes.toShort(Arrays.copyOfRange(raw, offset, offset + 2));
		offset += 2;
		//BYTE	SequenceSeries;	/* 3 fields encoded here...passed from fnet */
		SequenceSeries = raw[offset];
		offset += 1;
		//BYTE	Category;
		Category = raw[offset];
		offset += 1;
		//BYTE	SubCategory;	/* is this needed? */
		SubCategory = raw[offset];
		offset += 1;
		//WORD	LineID;	/* identifies line incoming data came in on (used for sequence numbers sets) */
		LineID = Bytes.toShort(Arrays.copyOfRange(raw, offset, offset + 2 + 1));
		offset += 2;
		//WORD	AuthCode;	/* authcode */
		AuthCode = Bytes.toShort(Arrays.copyOfRange(raw, offset, offset + 2 + 1));
		offset += 2;
		//TIME_T	ExchangeTime;	/* TIME_T representing time of transaction or transactions */
		ExchangeTime = Bytes.toInt(Arrays.copyOfRange(raw, offset, offset + 4 + 1));
		offset += 4;
		//TIME_T	Beacon;		/* beacon time, used for ticordering based upon beacon */
		Beacon = Bytes.toInt(Arrays.copyOfRange(raw, offset, offset + 4 + 1));
		offset += 4;
		//double	VWap;		/* vwap */
		//VWap = Bytes.toDouble(Arrays.copyOfRange(raw, offset, offset + 8 + 1));
		VWap = ByteBuffer.wrap(Arrays.copyOfRange(raw, offset, offset + 8 + 1)).getDouble();
		offset += 8;
		//DWORD32	SequenceNumber;	/* sequence number for LineID */
		SequenceNumber = Bytes.toInt(Arrays.copyOfRange(raw, offset, offset + 4 + 1));
		offset += 4;
		//BYTE	SecQualifiers[TICKDATA_SECQUAL_SIZE];
		SecQualifiers = Arrays.copyOfRange(raw, offset, offset + TICKDATA_SECQUAL_SIZE + 1);
		offset += TICKDATA_SECQUAL_SIZE;
		//PRICEDATA	Trade;
		Trade.fromBytes(Arrays.copyOfRange(raw, offset, offset + PriceDataType.RECORD_SIZE + 1));
		offset += PriceDataType.RECORD_SIZE;
		//PRICEDATA	Bid;
		Bid.fromBytes(Arrays.copyOfRange(raw, offset, offset + PriceDataType.RECORD_SIZE + 1));
		offset += PriceDataType.RECORD_SIZE;
		//PRICEDATA	Ask;
		Ask.fromBytes(Arrays.copyOfRange(raw, offset, offset + PriceDataType.RECORD_SIZE + 1));
		offset += PriceDataType.RECORD_SIZE;
	}
}
