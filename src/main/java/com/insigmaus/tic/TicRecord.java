package com.insigmaus.tic;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 25, 2013
 */

public class TicRecord {

    protected String symbol;

    protected int tTime; // TIME_T tTime;

    protected int uFlags; // unsigned uFlags;

    protected int exchangeTime; // TIME_T ExchangeTime;

    protected int sequenceNumber; // unsigned SequenceNumber;

    protected short lineID; // unsigned short LineID;

    protected byte sequenceSeries; // BYTE SequenceSeries; 
    
    // BYTE secqualifiers[TICKDATA_SECQUAL_SIZE];
    protected byte[] secqualifiers;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public int gettTime() {
        return tTime;
    }

    public void settTime(int tTime) {
        this.tTime = tTime;
    }

    public int getuFlags() {
        return uFlags;
    }

    public void setuFlags(int uFlags) {
        this.uFlags = uFlags;
    }

    public int getExchangeTime() {
        return exchangeTime;
    }

    public void setExchangeTime(int exchangeTime) {
        this.exchangeTime = exchangeTime;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public short getLineID() {
        return lineID;
    }

    public void setLineID(short lineID) {
        this.lineID = lineID;
    }

    public byte getSequenceSeries() {
        return sequenceSeries;
    }

    public void setSequenceSeries(byte sequenceSeries) {
        this.sequenceSeries = sequenceSeries;
    }

    public byte[] getSecqualifiers() {
        return secqualifiers;
    }

    public void setSecqualifiers(byte[] secqualifiers) {
        this.secqualifiers = secqualifiers;
    }

    // public static void main(String[] args) {
    // System.out.println("Short.MAX_VALUE : " + Short.MAX_VALUE);
    // System.out.println("Integer.MAX_VALUE : " + Integer.MAX_VALUE);
    // }
}


