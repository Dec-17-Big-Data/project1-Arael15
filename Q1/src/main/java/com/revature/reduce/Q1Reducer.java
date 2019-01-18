package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.logging.log4j.*;

public class Q1Reducer extends Reducer<Text, Text, Text, Text> {
	
	//private static Logger log = LogManager.getLogger(Q1Reducer.class);

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (Text value : values) {

			/*
			 * Add the value to the word count counter for this key.
			 */
			String parsed = value.toString();
			String[] split = parsed.split("%%");
			for (int i = 0; i < split.length; i++){
				try {
					if (Double.parseDouble(split[i]) < 30) {
						context.write(key, new Text(split[i]));
						i = split.length;
					}
					else {
						i = split.length;
					}
				}
				catch (NumberFormatException e){
					
				}
				catch (NullPointerException e){
					
				}
			}
		}

		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
}
}
