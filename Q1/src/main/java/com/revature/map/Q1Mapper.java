package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.logging.log4j.*;

public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	//private static Logger log = LogManager.getLogger(Q1Mapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/*
		 * Convert the line, which is received as a Text object,
		 * to a String object.
		 */
		String line = value.toString();
		String[] parsed = line.split("\",\"?");
		
		if (parsed[3].equals("SE.TER.CMPL.FE.ZS") & parsed.length > 5) {
			StringBuilder passing = new StringBuilder("");
			for (int i = parsed.length - 1; i > 0; i--){
				try {
					Double.parseDouble(parsed[i]);
					passing.append(parsed[i]);
					passing.append("%%");
				}
				catch (Exception e){
				}
			}
			if (!passing.toString().equals("")){
				context.write(new Text(parsed[0].substring(1)), new Text(passing.toString()));
			}
			
			
		}
		
		

		/*
		 * The line.split("\\W+") call uses regular expressions to split the
		 * line up by non-word characters.
		 * 
		 * If you are not familiar with the use of regular expressions in
		 * Java code, search the web for "Java Regex Tutorial." 
		 */
		
	}
}
