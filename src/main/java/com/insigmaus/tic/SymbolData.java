package com.insigmaus.tic;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 28, 2013
 */

public class SymbolData {

    private String symbol;

    private int startKey;
    private int endKey;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public int getStartKey() {
        return startKey;
    }

    public void setStartKey(int startKey) {
        this.startKey = startKey;
    }

    public int getEndKey() {
        return endKey;
    }

    public void setEndKey(int endKey) {
        this.endKey = endKey;
    }

    public SymbolData(String symbol, int startKey, int endKey) {
        super();
        this.symbol = symbol;
        this.startKey = startKey;
        this.endKey = endKey;
    }

}


