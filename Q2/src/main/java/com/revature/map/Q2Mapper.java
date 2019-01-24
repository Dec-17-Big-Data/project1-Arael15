package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author Eddie Smith
 * This is the mapper for Q2: List the average increase in female education in the U.S. from the year 2000.
 * For this, we use the data for Educational Attainment: at least Bachelor's or equivalent, as I believe it
 * gives a sufficient snapshot of the education of the whole populace. I tracked data for each year, and made a cutoff
 * beginning at 2004, since it was the first year from which data was readily available every year (except 2007)
 * It is also notable that there was a change in how this value was defined, leading to a significant drop in 2013.
 * For that reason, it was left out of final calculations.
 *
 */

public class Q2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	/**
	 * The map function here reads the selected row and parses out the value for each year, starting with the most recent.
	 * It then calculates the difference between that year and the year before, or divides the change evenly
	 * if data is only available from 2 years prior. If data is not available within the past two years, it
	 * ingores the rest of the data.
	 * Each year is passed as a key with the value being the percent change from the prior year.
	 */
	
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
