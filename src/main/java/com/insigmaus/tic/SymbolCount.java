package com.insigmaus.tic;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 25, 2013
 */

public class SymbolCount {

    private String symbol;

    private int count;

    public SymbolCount(String symbol, int count) {
        super();
        this.symbol = symbol;
        this.count = count;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String toString() {
        return symbol + "," + count;
    }

}


