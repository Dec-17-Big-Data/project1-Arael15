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

import com.revature.map.Q3Mapper;
import com.revature.reduce.Q3Reducer;

public class Q3Test {

	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		Q3Mapper mapper = new Q3Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Q3Reducer reducer = new Q3Reducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		/*
		 * Set up the mapper/reducer test harness.
		 */
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {

		/*
		 * For this test, the mapper's input will be "1 cat cat dog" 
		 */
		mapDriver.withInput(new LongWritable(1), new Text("\"Japan\",\"JPN\","
				+ "\"Employment to population ratio, 15+, male (%) (modeled ILO estimate)\","
				+ "\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"76.1080017089844\",\"76.447998046875\",\"76.3089981079102\","
				+ "\"75.7180023193359\",\"75.3010025024414\",\"75.1780014038086\",\"75.2030029296875\","
				+ "\"74.1429977416992\",\"73.197998046875\",\"72.6669998168945\",\"71.7949981689453\","
				+ "\"70.7740020751953\",\"70.2070007324219\",\"69.9410018920898\",\"70.0149993896484\","
				+ "\"69.9670028686523\",\"70.3109970092773\",\"69.9359970092773\",\"68.375\","
				+ "\"67.7269973754883\",\"67.5540008544922\",\"67.3119964599609\",\"67.4260025024414\","
				+ "\"67.802001953125\",\"67.6259994506836\",\"67.6129989624023\","));

		/*
		 * The expected output is "cat 1", "cat 1", and "dog 1".
		 */
		mapDriver.withOutput(new Text("Japan"), new Text("2016%%67.6129989624023"));
		mapDriver.withOutput(new Text("Japan"), new Text("2000%%72.6669998168945"));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		
		List<Text> values = new ArrayList<Text>();
		
		values.add(new Text("2016%%67.6129989624023"));
		values.add(new Text("2000%%72.6669998168945"));
		
		reduceDriver.withInput(new Text("Japan"), values);
		
		reduceDriver.withOutput(new Text("Japan"), new DoubleWritable(-06.955));
		
		reduceDriver.runTest();
		
	}
	
}
