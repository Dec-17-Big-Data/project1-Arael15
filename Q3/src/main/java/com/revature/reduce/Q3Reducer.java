package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.logging.log4j.*;


public class Q3Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	//private static Logger log = LogManager.getLogger(Q3Reducer.class);

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double earlier = 0, later = 0;

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (Text value : values) {

			/*
			 * Add the value to the word count counter for this key.
			 */
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

		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
	}
}
