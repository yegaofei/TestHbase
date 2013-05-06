package com.insigmaus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Starry Wu [XingWu@insigmaus.com]
 * @version V1.0 Create Time: Apr 20, 2013
 */

public class SymbolLoader {

	public static List<String> list = new ArrayList<String>();

	static {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				SymbolLoader.class.getResourceAsStream("Symbol.txt")));
		try {
			while (reader.ready()) {
				String s = reader.readLine();
				list.add(s.replace(" Symbol = ", ""));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static List<String> getSymbolList() {
		return list;
	}

	public static void main(String[] args) {
		System.out.println(SymbolLoader.getSymbolList());
	}

	public static String randomSymbol() {
		return list.get(RandomUtil.getInt() % list.size());
	}
}
