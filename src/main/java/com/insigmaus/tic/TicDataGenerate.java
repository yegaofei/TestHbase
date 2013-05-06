package com.insigmaus.tic;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;


/** 
 * 
 * @author  insigmaus12
 * @version V1.0  Create Time: Apr 25, 2013
 */

public class TicDataGenerate {

    private SymbolCount[] symbolCountArray;

    private static final int EXCHANGE_TIME_BASE = 1312000000;

    private static final short LINE_ID = 38;

    private static final byte[] SECQUALIFIERS = new byte[0];

    private static final byte SEQUENCESERIES = ' ';

    private static final byte[] VOLQUALIFIERS = new byte[] { ' ' };

    private static final int SEQUENCENUMBER_BASE = 248157845;

    private static final int FLAG_BASE = 268435472;

    private static final char[][] EXCHANGE_BASE = new char[][] { { 'B', 'A', 'T', 'S' },
            { 'P', 'S', 'E' }, { 'P', 'H', 'I', 'L' }, { 'N', 'M', 'S' }, { 'A', 'M', 'E', 'X' },
            { 'B', 'O', 'X' }, { 'C', 'B', 'O', 'E' } };

    private static final byte[] QUALIFIERS = new byte[] { (byte) '§' };

    public void readCountFile(String fileName, int lines) throws FileNotFoundException {
        symbolCountArray = new SymbolCount[lines];
        BufferedReader br = new BufferedReader(new InputStreamReader(
            TicDataGenerate.class.getResourceAsStream(fileName)));
        String line;
            try {
            int i = 0;
            while ((line = br.readLine()) != null && i < lines) {
                String[] keyValue = line.split("\t+");
                symbolCountArray[i++] = new SymbolCount(keyValue[0], Integer.valueOf(keyValue[1]));
                // System.out.println(keyValue[0] + "," + keyValue[1]);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
    }

    public SymbolCount[] getSymbolCountArray() {
        return symbolCountArray;
    }

    public void setSymbolCountArray(SymbolCount[] symbolCountArray) {
        this.symbolCountArray = symbolCountArray;
    }

    public TicQuoteCID[] generateTicQuoteCIDArray(String symbol, int size){
        TicQuoteCID[] tqcArray = new TicQuoteCID[size];
        
        char[] echange = EXCHANGE_BASE[(int) ((EXCHANGE_BASE.length - 1) * Math.random())];
        double randomPrice = Math.random() * 50;
        int randomSize = (int) Math.random() * 30;
        for(int i = 0; i < size; i++){
            TicQuoteCID tqc = new TicQuoteCID();
            tqc.setSymbol(symbol);
            int exchangeTime = EXCHANGE_TIME_BASE + i;
            tqc.settTime(exchangeTime);
            tqc.setExchangeTime(exchangeTime);
            tqc.setLineID(LINE_ID);
            tqc.setSecqualifiers(SECQUALIFIERS);
            tqc.setSequenceNumber((int) (SEQUENCENUMBER_BASE * Math.random()));
            tqc.setSequenceSeries(SEQUENCESERIES);
            tqc.setuFlags((int) (FLAG_BASE * Math.random()));
            tqc.setcAskExchange(echange);
            tqc.setcBidExchange(echange);
            tqc.setdAskPrice(randomPrice);
            tqc.setdBidPrice(randomPrice);
            tqc.setiAskSize(randomSize);
            tqc.setiBidSize(randomSize);

            tqcArray[i] = tqc;
        }

        return tqcArray;
    }

    public TicTradeCID[] generateTicTradeCID(String symbol, int size){
        TicTradeCID[] tqcArray = new TicTradeCID[size];
        
        char[] exchange = EXCHANGE_BASE[(int) ((EXCHANGE_BASE.length - 1) * Math.random())];
        double randomPrice = Math.random() * 50;
        int randomSize = (int) Math.random() * 100;
        for(int i = 0; i < size; i++){
            TicTradeCID tqc = new TicTradeCID();
            tqc.setSymbol(symbol);
            int exchangeTime = EXCHANGE_TIME_BASE + i;
            tqc.settTime(exchangeTime);
            tqc.setExchangeTime(exchangeTime);
            tqc.setLineID(LINE_ID);
            tqc.setSecqualifiers(SECQUALIFIERS);
            tqc.setSequenceNumber((int) (SEQUENCENUMBER_BASE * Math.random()));
            tqc.setSequenceSeries(SEQUENCESERIES);
            tqc.setuFlags((int) (FLAG_BASE * Math.random()));
            tqc.setcTradeExchange(exchange);
            tqc.setdTradePrice(randomPrice);
            tqc.setdVWAP(0d);
            tqc.setiTradeVolume(randomSize);
            tqc.setQualifiers(QUALIFIERS);
            tqc.setuCumVolume(randomSize);
            tqc.setVolqualifiers(VOLQUALIFIERS);

            tqcArray[i] = tqc;
        }
        return tqcArray;
    }

    public static int getExchangeBaseTime() {
        return EXCHANGE_TIME_BASE;
    }

    public static void main(String[] args) {
        TicDataGenerate tdg = new TicDataGenerate();
        try {
            tdg.readCountFile("TicSymbolAndCount.txt", 190009);
            SymbolCount[] m = tdg.getSymbolCountArray();
            System.out.println(m[1]);
            long maxCnt = 0;
            long maxCnt2 = 0;
            long totalCnt = 0;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < m.length; i++) {
                String symbol = m[i].getSymbol();
                int count = m[i].getCount();
                TicTradeCID[] s = tdg.generateTicTradeCID(symbol, count);
                TicQuoteCID[] q = tdg.generateTicQuoteCIDArray(symbol, count);
                totalCnt += count;
                if (count > maxCnt) {
                    maxCnt2 = maxCnt;
                    maxCnt = count;
                }
            }
            long endTime = System.currentTimeMillis();
            System.out.println(endTime - startTime);
            System.out.println(totalCnt);
            System.out.println("Max count" + maxCnt);
            System.out.println("Max count 2 " + maxCnt2);

            int t = 190009 * 800000;
            System.out.println("Max buckets " + t);

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}


