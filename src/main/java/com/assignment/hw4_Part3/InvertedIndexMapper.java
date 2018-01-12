package com.assignment.hw4_Part3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
	private Text range = new Text();
	private Text outkey = new Text();
	private String[] s = new String[] {"0-10", "10-20", "20-30", "30-40", "40-50", "50-60", "60-70", "70-80", "80-90", "90-100"};
	private List<String> ranges = new ArrayList<String>(Arrays.asList(s));
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		try{
		String[] fields = value.toString().split(",");
		double price = Double.parseDouble(fields[3]);
		String stock_name = fields[1];
		outkey.set(stock_name);

		if(price >= 0 && price <= 10){
			range.set(ranges.get(0));
			context.write(range, outkey);
			
		} else if(price > 10 && price <= 20){
			range.set(ranges.get(1));
			context.write(range, outkey);
			
		}else if(price > 20 && price <= 30){
			range.set(ranges.get(2));
			context.write(range, outkey);
			
		}else if(price > 30 && price <= 40){
			range.set(ranges.get(3));
			context.write(range, outkey);
			
		}else if(price > 40 && price <= 50){
			range.set(ranges.get(4));
			context.write(range, outkey);
			
		}else if(price > 50 && price <= 60){
			range.set(ranges.get(5));
			context.write(range, outkey);
			
		}else if(price > 60 && price <= 70){
			range.set(ranges.get(6));
			context.write(range, outkey);
			
		}else if(price > 70 && price <= 80){
			range.set(ranges.get(7));
			context.write(range, outkey);
			
		}else if(price > 80 && price <= 90){
			range.set(ranges.get(8));
			context.write(range, outkey);
			
		}else if(price > 90 && price <= 100){
			range.set(ranges.get(9));
			context.write(range, outkey);
		}
		}catch(NumberFormatException e){
			;
		}
	}
}
