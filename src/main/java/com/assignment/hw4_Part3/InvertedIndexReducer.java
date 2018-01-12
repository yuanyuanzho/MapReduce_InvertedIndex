package com.assignment.hw4_Part3;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		StringBuilder sb = new StringBuilder();
		HashSet<String> hashset = new HashSet<String>();
		boolean first = true;
		
		for(Text stock : values){
            hashset.add(stock.toString());
        }
		
		for(String s : hashset){
			if(first){
				first = false;
			}else{
				sb.append(" ");
			}
			sb.append(s.toString());
		}
		
		result.set(sb.toString());
		context.write(key, result);
		
	}
}
