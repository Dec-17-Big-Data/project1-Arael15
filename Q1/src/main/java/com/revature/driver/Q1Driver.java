package com.revature.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q1Mapper;
import com.revature.reduce.Q1Reducer;

/**
 * 
 * @author Eddie Smith
 * This is the driver for Q1: Identify the countries where % of female graduates is less than 30%.
 *
 */

public class Q1Driver {
	
	public static void main(String args[]) throws Exception {
		
		
		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(Q1Driver.class);
		
		job.setJobName("Q1");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Q1Mapper.class);
		job.setReducerClass(Q1Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}

}
