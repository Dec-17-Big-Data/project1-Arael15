package com.revature.reduce;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author Eddie Smith
 * A reducer for the custom Q5.
 *
 */

public class Q5Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

	/**
	 * Receives as input 4 values for each key, corresponding to the 4 desired series and the values
	 * in the 2014 column. Values are assumed to be either decimal numbers or empty, with any non-decimal
	 * value receiving the same treatment as an empty string. A nonzero value will be output from the reducer
	 * only if all four fields contain a decimal number. Using the data, we calculate a weighted average
	 * for the Gross Graduation Ratio based on gender prevalence in the country. The returned value is the quotient
	 * of this weighted average and the GDP percentage of education spending.
	 */
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double ggrm = 0, ggrf = 0, pop = 0, expend = 0;
		for (Text value : values) {
			String[] parsed = value.toString().split("%%");
			try {
				switch(parsed[0]){
				case "FeGGR":
					ggrf = Double.parseDouble(parsed[1]);
				case "MaGGR":
					ggrm = Double.parseDouble(parsed[1]);
				case "Pop":
					pop = Double.parseDouble(parsed[1]);
				case "Spend":
					expend = Double.parseDouble(parsed[1]);
				default:
					break;
				}
			}
			catch (NumberFormatException e){
			}
		}
		
		if (ggrm > 0 & ggrf > 0 & pop > 0 & expend > 0) {
			double result = (ggrm * (1 - (pop / 100)) + ggrf * (pop / 100)) / expend;
			DecimalFormat df = new DecimalFormat("00.####");
			String formatted = df.format(result);
			context.write(key, new DoubleWritable(Double.parseDouble(formatted)));
		}
		
		else {
			context.write(key, new DoubleWritable(-0.0));
		}
		
		
	}

}
