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

import com.revature.map.Q5Mapper;
import com.revature.reduce.Q5Reducer;

public class Q5Test {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	private ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp() {

		/*
		 * Set up the mapper test harness.
		 */
		Q5Mapper mapper = new Q5Mapper();
		mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
		mapDriver.setMapper(mapper);

		/*
		 * Set up the reducer test harness.
		 */
		Q5Reducer reducer = new Q5Reducer();
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
		
		mapDriver.withInput(new LongWritable(1), new Text("\"Hungary\",\"HUN\","
				+ "\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"29.51899\",\"31.63201\","
				+ "\"34.14055\",\"34.27807\",\"35.86189\",\"39.30007\",\"\",\"57.66757\",\"58.07336\","
				+ "\"53.51432\",\"51.12446\",\"53.30253\",\"48.57605\",\"40.05698\",\"37.48196\","
				+ "\"37.24785\",\"39.121\",\"\",\"\","));
		
		mapDriver.withInput(new LongWritable(2), new Text("\"Hungary\",\"HUN\","
				+ "\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"19.43787\",\"20.67976\","
				+ "\"21.81856\",\"19.92762\",\"21.23249\",\"22.61703\",\"\",\"31.22203\","
				+ "\"29.6819\",\"26.96388\",\"25.2288\",\"27.8216\",\"27.75244\",\"24.58862\","
				+ "\"23.20604\",\"23.30571\",\"24.41766\",\"\",\"\","));
		
		mapDriver.withInput(new LongWritable(3), new Text("\"Hungary\",\"HUN\","
				+ "\"Government expenditure on education, total (% of GDP)\",\"SE.XPD.TOTL.GD.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.90824\","
				+ "\"6.1094\",\"5.94864\",\"5.86352\",\"4.87127\",\"4.35096\",\"\",\"4.41695\",\"4.57082\","
				+ "\"4.87341\",\"4.93961\",\"5.18631\",\"5.81182\",\"5.35228\",\"5.35224\",\"5.33501\","
				+ "\"5.17582\",\"5.00656\",\"4.99572\",\"4.79943\",\"4.62877\",\"\",\"4.22919\",\"\",\"\",\"\","));
		
		mapDriver.withInput(new LongWritable(4), new Text("\"Hungary\",\"HUN\","
				+ "\"Population, female (% of total)\",\"SP.POP.TOTL.FE.ZS\","
				+ "\"51.7828035839448\",\"51.7757471069121\",\"51.748746381773\",\"51.7089358127659\","
				+ "\"51.6672178543775\",\"51.6315270323737\",\"51.6035111702349\",\"51.5808585467427\","
				+ "\"51.5628403705373\",\"51.5476790609873\",\"51.5341132561981\",\"51.5229145886923\","
				+ "\"51.5149769823024\",\"51.5092732298487\",\"51.5043959082428\",\"51.4996190239119\","
				+ "\"51.4939084796105\",\"51.488290978056\",\"51.4865257865917\",\"51.493638374136\","
				+ "\"51.5129163226918\",\"51.5461286088628\",\"51.5916370803085\",\"51.6451387361939\","
				+ "\"51.7001788363802\",\"51.7518768369357\",\"51.7982178605149\",\"51.84040546579\","
				+ "\"51.8804629559671\",\"51.9217752717761\",\"51.9666856073354\",\"52.0153809614316\","
				+ "\"52.0663145663905\",\"52.1180991870065\",\"52.1688649138212\",\"52.2172636498823\","
				+ "\"52.2628331486512\",\"52.3056858315698\",\"52.3457646390326\",\"52.3831208287047\","
				+ "\"52.4176522696883\",\"52.4493285489631\",\"52.477684869574\",\"52.5016637132818\","
				+ "\"52.5198985135531\",\"52.5314868396916\",\"52.5360171557939\",\"52.5339477658196\","
				+ "\"52.5263964227762\",\"52.5149549808465\",\"52.5008954397031\",\"52.4846613913159\","
				+ "\"52.466265943681\",\"52.4460509726075\",\"52.4242886929018\",\"52.4012780081792\",\"\","));
		
		mapDriver.withOutput(new Text("Hungary"), new Text("FeGGR%%39.121"));
		mapDriver.withOutput(new Text("Hungary"), new Text("MaGGR%%24.41766"));
		mapDriver.withOutput(new Text("Hungary"), new Text("Pop%52.4242886929018"));
		
	}
	
	@Test
	public void testReducer() {
		
		List<Text> values = new ArrayList<Text>();
		
		values.add(new Text("FeGGR%%27.5"));
		values.add(new Text("MaGGR%%18.7"));
		values.add(new Text("Pop%%48.0"));
		values.add(new Text("4.5"));
		
		reduceDriver.withInput(new Text("MyCountry"), values);
		
		reduceDriver.withOutput(new Text("MyCountry"), new DoubleWritable(05.0942));
		
	}
	
	@Test
	public void testReducer2() {
		
		List<Text> values = new ArrayList<Text>();
		
		values.add(new Text("FeGGR%%39.121"));
		values.add(new Text("MaGGR%%24.41766"));
		values.add(new Text("Pop%52.4242886929018"));
		
		reduceDriver.withInput(new Text("Hungary"), values);
		
		reduceDriver.withOutput(new Text("Hungary"), new DoubleWritable(-0.0));
		
	}
	
	@Test
	public void testMapReducer() {
		
		mapDriver.withInput(new LongWritable(1), new Text("\"Hungary\",\"HUN\","
				+ "\"Gross graduation ratio, tertiary, female (%)\",\"SE.TER.CMPL.FE.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"29.51899\",\"31.63201\","
				+ "\"34.14055\",\"34.27807\",\"35.86189\",\"39.30007\",\"\",\"57.66757\",\"58.07336\","
				+ "\"53.51432\",\"51.12446\",\"53.30253\",\"48.57605\",\"40.05698\",\"37.48196\","
				+ "\"37.24785\",\"39.121\",\"\",\"\","));
		
		mapDriver.withInput(new LongWritable(2), new Text("\"Hungary\",\"HUN\","
				+ "\"Gross graduation ratio, tertiary, male (%)\",\"SE.TER.CMPL.MA.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"19.43787\",\"20.67976\","
				+ "\"21.81856\",\"19.92762\",\"21.23249\",\"22.61703\",\"\",\"31.22203\","
				+ "\"29.6819\",\"26.96388\",\"25.2288\",\"27.8216\",\"27.75244\",\"24.58862\","
				+ "\"23.20604\",\"23.30571\",\"24.41766\",\"\",\"\","));
		
		mapDriver.withInput(new LongWritable(3), new Text("\"Hungary\",\"HUN\","
				+ "\"Government expenditure on education, total (% of GDP)\",\"SE.XPD.TOTL.GD.ZS\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"5.90824\","
				+ "\"6.1094\",\"5.94864\",\"5.86352\",\"4.87127\",\"4.35096\",\"\",\"4.41695\",\"4.57082\","
				+ "\"4.87341\",\"4.93961\",\"5.18631\",\"5.81182\",\"5.35228\",\"5.35224\",\"5.33501\","
				+ "\"5.17582\",\"5.00656\",\"4.99572\",\"4.79943\",\"4.62877\",\"\",\"4.22919\",\"\",\"\",\"\","));
		
		mapDriver.withInput(new LongWritable(4), new Text("\"Hungary\",\"HUN\","
				+ "\"Population, female (% of total)\",\"SP.POP.TOTL.FE.ZS\","
				+ "\"51.7828035839448\",\"51.7757471069121\",\"51.748746381773\",\"51.7089358127659\","
				+ "\"51.6672178543775\",\"51.6315270323737\",\"51.6035111702349\",\"51.5808585467427\","
				+ "\"51.5628403705373\",\"51.5476790609873\",\"51.5341132561981\",\"51.5229145886923\","
				+ "\"51.5149769823024\",\"51.5092732298487\",\"51.5043959082428\",\"51.4996190239119\","
				+ "\"51.4939084796105\",\"51.488290978056\",\"51.4865257865917\",\"51.493638374136\","
				+ "\"51.5129163226918\",\"51.5461286088628\",\"51.5916370803085\",\"51.6451387361939\","
				+ "\"51.7001788363802\",\"51.7518768369357\",\"51.7982178605149\",\"51.84040546579\","
				+ "\"51.8804629559671\",\"51.9217752717761\",\"51.9666856073354\",\"52.0153809614316\","
				+ "\"52.0663145663905\",\"52.1180991870065\",\"52.1688649138212\",\"52.2172636498823\","
				+ "\"52.2628331486512\",\"52.3056858315698\",\"52.3457646390326\",\"52.3831208287047\","
				+ "\"52.4176522696883\",\"52.4493285489631\",\"52.477684869574\",\"52.5016637132818\","
				+ "\"52.5198985135531\",\"52.5314868396916\",\"52.5360171557939\",\"52.5339477658196\","
				+ "\"52.5263964227762\",\"52.5149549808465\",\"52.5008954397031\",\"52.4846613913159\","
				+ "\"52.466265943681\",\"52.4460509726075\",\"52.4242886929018\",\"52.4012780081792\",\"\","));
		
		mapReduceDriver.withOutput(new Text("Hungary"), new DoubleWritable(-0.0));
		
	}

}
