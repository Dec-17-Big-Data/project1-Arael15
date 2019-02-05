package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author Eddie Smith
 * The mapper for Q3: List the % of change in male employment from the year 2000.
 * The series selected to answer this query is Employment to Population ratio (Male) 15+, since
 * there was no specific entry found for employment rate. Using 2000 as a base year, we also selected
 * the most recent year for which each country had this data. After verification, this was determined to
 * be 2016 for every country that also had data in 2000, though the class is set up in such a way that
 * it would find the most recent year that a country had data. It then sends two values out for each country
 * corresponding to the year and the ratio combined with a delimiter.
 *
 */

public class Q3Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	/**
	 * The map function parses for the rows relating to the employment to population ratio as desired,
	 * then extracts the data from the column relating to the year 2000 and the most recent year containing data
	 * and then sends these to the mapper marked with a delimiter.
	 */

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String line = value.toString();
		String[] parsed = line.split("\",\"?");
		String name = parsed[0].substring(1);
		
		if (parsed[3].equals("SL.EMP.TOTL.SP.MA.ZS")) {
			loop: for (int i = 1; i <= parsed.length; i++) {
				double percent = 0;
				try {
					percent = Double.parseDouble(parsed[parsed.length - i]);
					String converted = (2017 -i) + "%%" + percent;
					context.write(new Text(name), new Text(converted));
					break loop;
				}
				catch (Exception e){
					e.printStackTrace(System.out);
				}
			}
			try {
				double two_percent = Double.parseDouble(parsed[parsed.length - 17]);
				context.write(new Text(name), new Text("2000%%" + two_percent));
			}
			catch (Exception e){
				
			}
		}
	}
}
