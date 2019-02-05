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

import com.revature.map.Q4Mapper;
import com.revature.reduce.Q4Reducer;

public class Q4Test {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		Q4Mapper mapper = new Q4Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Q4Reducer reducer = new Q4Reducer();
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
		
		mapDriver.withInput(new LongWritable(1), new Text("\"Mali\",\"MLI\","
				+ "\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\","
				+ "\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"32.6660003662109\",\"32.5610008239746\","
				+ "\"30.113000869751\",\"30.386999130249\",\"32.6549987792969\",\"32.3959999084473\","
				+ "\"34.7229995727539\",\"32.5810012817383\",\"31.5340003967285\",\"32.2750015258789\","
				+ "\"32.1990013122559\",\"32.3380012512207\",\"33.6590003967285\",\"31.5450000762939\","
				+ "\"33.0589981079102\",\"34.5769996643066\",\"35.8390007019043\",\"38.609001159668\","
				+ "\"41.4970016479492\",\"45.2309989929199\",\"45.3269996643066\",\"45.3160018920898\","
				+ "\"44.9620018005371\",\"44.0750007629395\",\"44.3289985656738\",\"44.564998626709\","));
		
		mapDriver.withOutput(new Text("Mali"), new Text("2016%%44.564998626709"));
		mapDriver.withOutput(new Text("Mali"), new Text("2000%%32.2750015258789"));

		/*
		 * Run the test.
		 */
		mapDriver.runTest();
		
	}
	
	@Test
	public void testReducer() {
		
		List<Text> values = new ArrayList<Text>();
		
		values.add(new Text("2016%%44.564998626709"));
		values.add(new Text("2000%%32.2750015258789"));
		
		reduceDriver.withInput(new Text("Mali"), values);
		
		reduceDriver.withOutput(new Text("Mali"), new DoubleWritable(38.079));
		
		reduceDriver.runTest();
		
	}
	
	@Test
	public void testMapReducer() {
		
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"Mali\",\"MLI\","
				+ "\"Employment to population ratio, 15+, female (%) (modeled ILO estimate)\","
				+ "\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"32.6660003662109\",\"32.5610008239746\","
				+ "\"30.113000869751\",\"30.386999130249\",\"32.6549987792969\",\"32.3959999084473\","
				+ "\"34.7229995727539\",\"32.5810012817383\",\"31.5340003967285\",\"32.2750015258789\","
				+ "\"32.1990013122559\",\"32.3380012512207\",\"33.6590003967285\",\"31.5450000762939\","
				+ "\"33.0589981079102\",\"34.5769996643066\",\"35.8390007019043\",\"38.609001159668\","
				+ "\"41.4970016479492\",\"45.2309989929199\",\"45.3269996643066\",\"45.3160018920898\","
				+ "\"44.9620018005371\",\"44.0750007629395\",\"44.3289985656738\",\"44.564998626709\","));
		
		mapReduceDriver.withOutput(new Text("Mali"), new DoubleWritable(38.079));
		
		mapReduceDriver.runTest();
		
	}

}
