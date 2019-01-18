package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Q2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String line = value.toString();
		String[] parsed = line.split("\",\"");
		
		if (parsed[1].equals("USA") & parsed[3].equals("SE.TER.HIAT.BA.FE.ZS")) {
			for (int i = 1; i <= parsed.length; i++) {
				double percent = 0;
				try {
					percent = Double.parseDouble(parsed[parsed.length - i]);
					String prev = parsed[parsed.length - i - 1];
					String prev2 = parsed[parsed.length - i - 2];
					double percent_change = 0;
					if (!prev.equals("")){
						percent_change = percent - Double.parseDouble(prev);
						context.write(new Text("change"), new DoubleWritable(percent_change));
					}
					else if(!prev2.equals("")){
						percent_change = (percent - Double.parseDouble(prev2)) / 2;
						context.write(new Text("change"), new DoubleWritable(percent_change));
						context.write(new Text("change"), new DoubleWritable(percent_change));
					}
					else {
						i = parsed.length;
					}
				}
				catch (Exception e){
					
				}
			}
		}
	}
}
