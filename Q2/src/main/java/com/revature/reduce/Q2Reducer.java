package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/*
 * This reducer for Q2 is simply performing an average of all the data, omitting the data
 * from 2013 as it is an enormous outlier
 */

public class Q2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		double sum = 0;
		int count = 0;
		
		for (DoubleWritable value : values){
			double extracted = value.get();
			if (Math.abs(extracted) < 10){
				sum += extracted;
				count++;
			}
		}
		context.write(new Text("Average"), new DoubleWritable(sum/count));
	}
}
