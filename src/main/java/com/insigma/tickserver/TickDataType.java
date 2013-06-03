package com.insigma.tickserver;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
        Flags = EndienConvertion.unsignedShortToInt(Arrays.copyOfRange(raw, offset, offset + 2),
                                                    ByteOrder.LITTLE_ENDIAN);
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
        LineID = EndienConvertion.unsignedShortToInt(Arrays.copyOfRange(raw, offset, offset + 2),
                                                     ByteOrder.LITTLE_ENDIAN);
		offset += 2;
		//WORD	AuthCode;	/* authcode */
        AuthCode = EndienConvertion.unsignedShortToInt(Arrays.copyOfRange(raw, offset, offset + 2),
                                                       ByteOrder.LITTLE_ENDIAN);
		offset += 2;

		//TIME_T	ExchangeTime;	/* TIME_T representing time of transaction or transactions */
        ExchangeTime = EndienConvertion.unsignedIntToLong(Arrays.copyOfRange(raw, offset,
                                                                             offset + 4),
                                                          ByteOrder.LITTLE_ENDIAN);
        offset += 4;

		//TIME_T	Beacon;		/* beacon time, used for ticordering based upon beacon */
        Beacon = EndienConvertion.unsignedIntToLong(Arrays.copyOfRange(raw, offset, offset + 4),
                                                    ByteOrder.LITTLE_ENDIAN);
        offset += 4;

		//double	VWap;		/* vwap */
		//VWap = Bytes.toDouble(Arrays.copyOfRange(raw, offset, offset + 8 + 1));
        VWap = ByteBuffer.wrap(Arrays.copyOfRange(raw, offset, offset + 8))
                         .order(ByteOrder.LITTLE_ENDIAN).getDouble();
		offset += 8;
		//DWORD32	SequenceNumber;	/* sequence number for LineID */
        SequenceNumber = EndienConvertion.unsignedIntToLong(Arrays.copyOfRange(raw, offset,
                                                                               offset + 4),
                                                            ByteOrder.LITTLE_ENDIAN);
		offset += 4;
		//BYTE	SecQualifiers[TICKDATA_SECQUAL_SIZE];
        SecQualifiers = Arrays.copyOfRange(raw, offset, offset + TICKDATA_SECQUAL_SIZE);
		offset += TICKDATA_SECQUAL_SIZE;
		//PRICEDATA	Trade;
        Trade.fromBytes(Arrays.copyOfRange(raw, offset, offset + PriceDataType.RECORD_SIZE));
		offset += PriceDataType.RECORD_SIZE;
		//PRICEDATA	Bid;
        Bid.fromBytes(Arrays.copyOfRange(raw, offset, offset + PriceDataType.RECORD_SIZE));
		offset += PriceDataType.RECORD_SIZE;
		//PRICEDATA	Ask;
        Ask.fromBytes(Arrays.copyOfRange(raw, offset, offset + PriceDataType.RECORD_SIZE));
		offset += PriceDataType.RECORD_SIZE;
	}

    public byte[] toBytes() {
        byte[] objInArray = new byte[RECORD_SIZE];

        int offset = 0;
        System.arraycopy(Bytes.toBytes(Flags), 2, objInArray, offset, 2);
        offset += 2;

        objInArray[offset] = SequenceSeries;
        offset++;

        objInArray[offset] = Category;
        offset++;

        objInArray[offset] = SubCategory;
        offset++;

        System.arraycopy(Bytes.toBytes(LineID), 2, objInArray, offset, 2);
        offset += 2;

        System.arraycopy(Bytes.toBytes(AuthCode), 2, objInArray, offset, 2);
        offset += 2;

        System.arraycopy(Bytes.toBytes(ExchangeTime), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(Bytes.toBytes(Beacon), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(Bytes.toBytes(VWap), 0, objInArray, offset, 8);
        offset += 8;

        System.arraycopy(Bytes.toBytes(SequenceNumber), 4, objInArray, offset, 4);
        offset += 4;

        System.arraycopy(SecQualifiers, 0, objInArray, offset, TICKDATA_SECQUAL_SIZE);
        offset += TICKDATA_SECQUAL_SIZE;

        System.arraycopy(Trade.toBytes(), 0, objInArray, offset, PriceDataType.RECORD_SIZE);
        offset += PriceDataType.RECORD_SIZE;

        System.arraycopy(Bid.toBytes(), 0, objInArray, offset, PriceDataType.RECORD_SIZE);
        offset += PriceDataType.RECORD_SIZE;

        System.arraycopy(Ask.toBytes(), 0, objInArray, offset, PriceDataType.RECORD_SIZE);
        offset += PriceDataType.RECORD_SIZE;

        return objInArray;

    }
}
