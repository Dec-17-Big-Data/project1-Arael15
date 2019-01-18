package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.logging.log4j.*;

public class Q3Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	//private static Logger log = LogManager.getLogger(Q3Mapper.class);

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
