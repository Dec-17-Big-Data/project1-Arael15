package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Q1Mapper;
import com.revature.reduce.Q1Reducer;

public class Q1Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		Q1Mapper mapper = new Q1Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Q1Reducer reducer = new Q1Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		mapDriver.withInput(new LongWritable(1), new Text("\"Croatia\",\"HRV\",\"Gross graduation ratio,"
				+ " tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.40078\",\"\",\"18.49693\","
				+ "\"17.23575\",\"19.49629\",\"\",\"19.64021\",\"21.74721\",\"22.38147\",\"32.18853\",\"43.53433\","
				+ "\"51.2139\",\"57.05251\",\"\",\"\",\"56.56231\",\"\",\"\",\""));

		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		mapDriver.withOutput(new Text("Croatia"), new Text("56.56231%%57.05251%%51.2139%%43.53433%%32.18853%%22.38147%%"
				+ "21.74721%%19.64021%%19.49629%%17.23575%%18.49693%%17.40078%%"));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text("56.56231%%57.05251%%51.2139%%43.53433%%32.18853%%22.38147%%"
				+ "21.74721%%19.64021%%19.49629%%17.23575%%18.49693%%17.40078%%"));

		/*
		 * For this test, the reducer's input will be "cat 1 1".
		 */
		reduceDriver.withInput(new Text("Croatia"), values);

		/*
		 * The expected output is "cat 2"
		 */

		/*
		 * Run the test.
		 */
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducer() {
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Croatia\",\"HRV\",\"Gross graduation ratio,"
				+ " tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"17.40078\",\"\",\"18.49693\","
				+ "\"17.23575\",\"19.49629\",\"\",\"19.64021\",\"21.74721\",\"22.38147\",\"32.18853\",\"43.53433\","
				+ "\"51.2139\",\"57.05251\",\"\",\"\",\"56.56231\",\"\",\"\",\""));
		
		mapReduceDriver.runTest();

	}
	
}
