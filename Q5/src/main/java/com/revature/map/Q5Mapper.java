package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Eddie Smith
 * This is the Mapper for a custom MapReduce task for project 1.
 * In it, I attempt to examine the relationship between a measure of educational performance and
 * government funding for education. The basic idea is that the relationship between the two
 * could represent a measure of rate of return for education spending. The data chosen is the
 * Gross Graduation Rate for tertiary education and education spending as a percentage of GDP.
 * One assumption is that a country's income group affects the quality of education given that the gulf
 * in GDP is significantly large. As a result, the results are partitioned by income group.
 * Additionally, data is selected from 2014 since in trial runs of the last few years of available data,
 * it seemed to be the least sparse.
 *
 */

public class Q5Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * The map functions scans the CSV of country data and extracts the rows which correspond to one of
	 * four data series: Gross Graduation Rate - Tertiary (Male), Gross Graduation Rate - Tertiary (Female),
	 * Female percent of population, and Government spending as percentage of GDP. From these four rows, we
	 * extract the column corresponding to 2014. The mapper then sends the key value pair of <C, V> where C
	 * is the country name, and V is the data series combined with the value via a delimiter.
	 */
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		String line = value.toString();
		String[] parsed = line.split("\",\"?");
		String name = parsed[0].substring(1);
		
		if (parsed[3].equals("SE.TER.CMPL.FE.ZS")) {
			if (!parsed[parsed.length - 4].equals("")){
				context.write(new Text(name), new Text("FeGGR%%" + parsed[parsed.length - 4]));
			}
		}
		else if (parsed[3].equals("SE.TER.CMPL.MA.ZS")) {
			if (!parsed[parsed.length - 4].equals("")){
				context.write(new Text(name), new Text("MaGGR%%" + parsed[parsed.length - 4]));
			}
		}
		else if (parsed[3].equals("SP.POP.TOTL.FE.ZS")) {
			if (!parsed[parsed.length - 4].equals("")){
				context.write(new Text(name), new Text("Pop%%" + parsed[parsed.length - 4]));
			}
		}
		else if (parsed[3].equals("SE.XPD.TOTL.GD.ZS")) {
			if (!parsed[parsed.length - 4].equals("")){
				context.write(new Text(name), new Text("Spend%%" + parsed[parsed.length - 4]));
			}
		}
	}
	
}
