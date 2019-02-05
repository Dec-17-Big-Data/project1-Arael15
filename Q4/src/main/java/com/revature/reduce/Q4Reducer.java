package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 * @author Eddie Smith
 * The reducer for Q4: List the % of change in female employment from the year 2000.
 * 
 *
 */

public class Q4Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	/**
	 * Receives as input two values for each country of year and employment percentage
	 * separated by a delimiter. After separating the two, computes the percent change as
	 * (2016 % - 2000 %) / (2000 %). Assumption was that a number adjusted to the previous employment
	 * would be more useful that a raw absolute change, though this value could be just as easily computed.
	 */
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double earlier = 0, later = 0;

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (Text value : values) {

			String parsed = value.toString();
			String[] split = parsed.split("%%");
			if (split[0].equals("2000")) {
				earlier = Double.parseDouble(split[1]);
			}
			else {
				later = Double.parseDouble(split[1]);
			}
		}
		double ratio = 100 * (later - earlier) / earlier;
		DecimalFormat df = new DecimalFormat("00.####");
		String formatted = df.format(ratio);
		
		context.write(key, new DoubleWritable(Double.parseDouble(formatted)));

	}
}
