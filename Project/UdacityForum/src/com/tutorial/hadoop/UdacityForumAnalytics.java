package com.tutorial.hadoop;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UdacityForumAnalytics {
	public static class StudentTimesMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			System.out.println("token length is" + tokens.length);
			if(tokens.length>=4) {
				String StudentId = tokens[3];
				try {
					String dateValue =  tokens[8];
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Date date = dateFormat.parse(dateValue);
					context.write(new Text(StudentId), new Text(String.valueOf(date.getHours())));
				}
				catch(Exception e) {
					System.out.println("Exception");
				}
			}
		}
	}

	public static class PricePerDayMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			System.out.println("token length is" + tokens.length);
			if(tokens.length==6) {
				String date = tokens[0];
				try {
					BigDecimal itemPrice =  new BigDecimal(tokens[4]);
					if(!getDayFromDate(date).toString().equals("Sunday") && !getDayFromDate(date).toString().equals("Saturday"))
						context.write(new Text(getDayFromDate(date)), new Text(itemPrice.toString()));
				}
				catch(Exception e) {
					System.out.println("Exception");
				}
			}
		}

		private String getDayFromDate(String dateValue) throws ParseException {
			Date date = null;
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			date =  dateFormat.parse(dateValue);
			dateFormat = new SimpleDateFormat("EEEEE");
			return dateFormat.format(date);
		}
	}

	public static class IntSumReducer
	extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			context.write(key, new Text(getMean(values).toString()));
		}
	}

	public static class StudentTimesReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			for(Text value:values)
				context.write(key, value);
		}
	}

	private static BigDecimal getMax(Iterable<Text> values) {
		BigDecimal maxValue = null;
		for (Text value : values) {
			if(maxValue==null)
				maxValue = new BigDecimal(value.toString());
			else {
				BigDecimal currentValue = new BigDecimal(value.toString());
				maxValue = maxValue.max(currentValue);
			}
		}
		if(maxValue==null) 
			maxValue = new BigDecimal(0);
		return maxValue;
	}

	private static BigDecimal getSum(Iterable<Text> values) {
		BigDecimal sum = new BigDecimal(0);
		int totalNum = 0;
		for (Text value : values) {
			sum = sum.add(new BigDecimal(value.toString()));
		}
		return sum;
	}
	private static BigDecimal getMean(Iterable<Text> values) {
		BigDecimal sum = new BigDecimal(0);
		int totalNum = 0;
		for (Text value : values) {
			sum = sum.add(new BigDecimal(value.toString()));
			totalNum++;
		}
		return sum.divide(new BigDecimal(totalNum),MathContext.DECIMAL128);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "UdacityForumAnalytics");
		job.setJarByClass(UdacityForumAnalytics.class);
		job.setMapperClass(StudentTimesMapper.class);
		job.setCombinerClass(StudentTimesReducer.class);
		job.setReducerClass(StudentTimesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}