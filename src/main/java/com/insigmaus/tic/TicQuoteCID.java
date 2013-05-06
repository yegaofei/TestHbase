package com.insigmaus.tic;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 25, 2013
 */

public class TicQuoteCID extends TicRecord {

    // unsigned iBidSize;
    // unsigned iAskSize;
    // char cBidExchange[TICKDATA_EXG_SIZE];
    // char cAskExchange[TICKDATA_EXG_SIZE];
    //
    // BYTE bidqualifiers[TICKDATA_QUAL_SIZE];
    // BYTE bidvolqualifiers[TICKDATA_VOLQUAL_SIZE];
    //
    // BYTE askqualifiers[TICKDATA_QUAL_SIZE];
    // BYTE askvolqualifiers[TICKDATA_VOLQUAL_SIZE];

    private double dBidPrice; // 8 byte
    private double dAskPrice; // 8 byte
    private int iBidSize; // 4 byte
    private int iAskSize; // 4 byte
    private char[] cBidExchange; // 4 byte
    private char[] cAskExchange; // 4 byte

    public double getdBidPrice() {
        return dBidPrice;
    }

    public void setdBidPrice(double dBidPrice) {
        this.dBidPrice = dBidPrice;
    }

    public double getdAskPrice() {
        return dAskPrice;
    }

    public void setdAskPrice(double dAskPrice) {
        this.dAskPrice = dAskPrice;
    }

    public int getiBidSize() {
        return iBidSize;
    }

    public void setiBidSize(int iBidSize) {
        this.iBidSize = iBidSize;
    }

    public int getiAskSize() {
        return iAskSize;
    }

    public void setiAskSize(int iAskSize) {
        this.iAskSize = iAskSize;
    }

    public char[] getcBidExchange() {
        return cBidExchange;
    }

    public void setcBidExchange(char[] cBidExchange) {
        this.cBidExchange = cBidExchange;
    }

    public char[] getcAskExchange() {
        return cAskExchange;
    }

    public void setcAskExchange(char[] cAskExchange) {
        this.cAskExchange = cAskExchange;
    }

    // private byte[] bidqualifiers;
    // private byte[] bidvolqualifiers;
    // private byte[] askqualifiers;
    // private byte[] askvolqualifiers;
}


