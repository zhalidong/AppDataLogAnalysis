package com.doit.json.rate.parse;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

public class Test {
	
	public static void main(String[] args) throws Exception {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("d:/rating.json"),"UTF-8"));
		
		String line = br.readLine();
		MovieRate mrObj = MovieRateLogParse.parseLine(line);
		
		System.out.println(mrObj);
		
	}
	
}
