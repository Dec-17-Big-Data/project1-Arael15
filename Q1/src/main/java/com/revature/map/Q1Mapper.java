package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Eddie Smith
 * This is the mapper for Q1: Identify the countries where % of female graduates is less than 30%. 
 * We use the Gross Graduation Rate - Tertiary (Female) as a measure of this statistic, as
 * it provides data as to the current graduation rate. We assume that for each country it will have data
 * for some year, though this may not be the case.
 * 
 *
 */

public class Q1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * The mapper finds the rows which correspond to the desired series and begins iterating
	 * from the most recent year backwards until it finds a year with data. For each year of data found,
	 * we add that year of data to a delimited string which will be passed as a value for that country key.
	 * In actuality, this is unnecessary, since we could have just sent the most recent as the key and not needed
	 * a reducer for this question. If no data is found, then the key is not sent to the reducer.
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
		
	}
}
