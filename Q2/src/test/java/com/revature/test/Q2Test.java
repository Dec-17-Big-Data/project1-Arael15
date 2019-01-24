package com.revature.test;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Q2Mapper;
import com.revature.reduce.Q2Reducer;


public class Q2Test {
	
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {
		
		Q2Mapper mapper = new Q2Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Q2Reducer reducer = new Q2Reducer();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
	}
	
	@Test
	public void testMapper() {
		
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\","
				+ "\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\","
				+ "\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\","
				+ "\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\","
				+ "\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\","
				+ "\"20.18248\",\"20.38445\",\"20.68499\",\"\","));
		
		mapDriver.withOutput(new Text("change"), new DoubleWritable(-20.939829999999997));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.20196999999999932));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.30053999999999803));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.4590199999999989));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.4590199999999989));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.5909899999999979));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.630510000000001));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.6321000000000012));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.7123000000000062));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(0.7462499999999963));
		mapDriver.withOutput(new Text("change"), new DoubleWritable(1.5175899999999984));
		
		mapDriver.runTest(false);
		
	}
	
	@Test
	public void testReducer() {
		
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		
		values.add(new DoubleWritable(0.630510000000001));
		values.add(new DoubleWritable(1.5175899999999984));
		values.add(new DoubleWritable(0.4590199999999989));
		values.add(new DoubleWritable(0.4590199999999989));
		values.add(new DoubleWritable(0.7123000000000062));
		values.add(new DoubleWritable(0.7462499999999963));
		values.add(new DoubleWritable(0.6321000000000012));
		values.add(new DoubleWritable(0.5909899999999979));
		values.add(new DoubleWritable(-20.939829999999997));
		values.add(new DoubleWritable(0.20196999999999932));
		values.add(new DoubleWritable(0.30053999999999803));
		
		reduceDriver.withInput(new Text("change"), values);
		
		reduceDriver.withOutput(new Text("Average"), new DoubleWritable(0.6250289999999996));
		
		reduceDriver.runTest();
		
	}
	
	@Test
	public void testMapReducer() {
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\","
				+ "\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\","
				+ "\"SE.TER.HIAT.BA.FE.ZS\",\"14.8\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"18.7\",\"\",\"\","
				+ "\"\",\"\",\"22.2\",\"\",\"\",\"\",\"26.9\",\"28.10064\",\"28.02803\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"44.54951\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"35.37453\","
				+ "\"36.00504\",\"37.52263\",\"\",\"38.44067\",\"39.15297\",\"39.89922\",\"40.53132\",\"41.12231\","
				+ "\"20.18248\",\"20.38445\",\"20.68499\",\"\","));
		
		mapReduceDriver.withOutput(new Text("Average"), new DoubleWritable(0.6250289999999996));
		
		mapReduceDriver.runTest();
		
	}

}
