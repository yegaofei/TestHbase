package com.insigmaus.tic;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 25, 2013
 */

public class TicTradeCID extends TicRecord {

    // double dTradePrice;
    // unsigned iTradeVolume;
    // unsigned uCumVolume;
    // char cTradeExchange[TICKDATA_EXG_SIZE];
    // double dVWAP;
    // BYTE qualifiers[TICKDATA_QUAL_SIZE];
    // BYTE volqualifiers[TICKDATA_VOLQUAL_SIZE];

    private double dTradePrice; // 8 bytes
    private int iTradeVolume; // 4 bytes
    private int uCumVolume; // 4 bytes
    private char[] cTradeExchange; // 4 byte
    private double dVWAP; // 8 bytes

    private byte[] qualifiers;
    private byte[] volqualifiers;

    public double getdTradePrice() {
        return dTradePrice;
    }

    public void setdTradePrice(double dTradePrice) {
        this.dTradePrice = dTradePrice;
    }

    public int getiTradeVolume() {
        return iTradeVolume;
    }

    public void setiTradeVolume(int iTradeVolume) {
        this.iTradeVolume = iTradeVolume;
    }

    public int getuCumVolume() {
        return uCumVolume;
    }

    public void setuCumVolume(int uCumVolume) {
        this.uCumVolume = uCumVolume;
    }

    public char[] getcTradeExchange() {
        return cTradeExchange;
    }

    public void setcTradeExchange(char[] cTradeExchange) {
        this.cTradeExchange = cTradeExchange;
    }

    public double getdVWAP() {
        return dVWAP;
    }

    public void setdVWAP(double dVWAP) {
        this.dVWAP = dVWAP;
    }

    public byte[] getQualifiers() {
        return qualifiers;
    }

    public void setQualifiers(byte[] qualifiers) {
        this.qualifiers = qualifiers;
    }

    public byte[] getVolqualifiers() {
        return volqualifiers;
    }

    public void setVolqualifiers(byte[] volqualifiers) {
        this.volqualifiers = volqualifiers;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.symbol).append(",").append(this.dTradePrice).append(",")
          .append(this.dVWAP).append(",").append(this.exchangeTime).append(",")
          .append(this.iTradeVolume).append(",").append(this.sequenceNumber).append(",")
.append(this.cTradeExchange).append(",")
          .append(this.tTime).append(",").append(this.uCumVolume)
.append(",").append(this.uFlags);
        return sb.toString();
    }

}


