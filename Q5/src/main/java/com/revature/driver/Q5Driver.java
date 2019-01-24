package com.revature.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Q5Mapper;
import com.revature.partition.Q5Partitioner;
import com.revature.reduce.Q5Reducer;

/**
 * 
 * @author Eddie Smith
 * This is the driver for a custom question 5 which attempts to calculate a representation of rate
 * of return for education spending. See Q5Mapper for a more detailed explanation.
 *
 */

public class Q5Driver {
	
	public static void main(String args[]) throws Exception {
		

		if (args.length != 2) {
			System.out.printf(
					"Usage: WordCount <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(Q5Driver.class);
		
		job.setJobName("Q4");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(Q5Mapper.class);
		job.setPartitionerClass(Q5Partitioner.class);
		job.setReducerClass(Q5Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(5);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}

}
