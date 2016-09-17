package com.tutorial.hadoop;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PurchaseSalesJob {
	public static class StoreMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			System.out.println("token length is" + tokens.length);
			if(tokens.length==6) {
				String storeName = tokens[2];
				try {
					BigDecimal itemPrice =  new BigDecimal(tokens[4]);
					context.write(new Text(storeName), new Text(itemPrice.toString()));
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
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			dateFormat = new SimpleDateFormat("EEEEE");
			return dateFormat.format(dateFormat.parse(dateValue));
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

	public static class SumOfPricePerWeekDayReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			context.write(key, new Text(getSum(values).toString()));
		}
	}

	private static BigDecimal getSum(Iterable<Text> values) {
		return StreamSupport.stream(values.spliterator(), false)
				.map(t-> new BigDecimal(t.toString()))
				.max(Comparator.naturalOrder())
				.get();
	}
	
	private static BigDecimal getMean(Iterable<Text> values) {
		List<BigDecimal> numList = StreamSupport.stream(values.spliterator(), false)
				.map(t-> new BigDecimal(t.toString()))
				.collect(Collectors.toList());
		BigDecimal sum = numList.stream()
					            .reduce(BigDecimal.ZERO, (BigDecimal::add));
		return sum.divide(new BigDecimal(numList.size()),MathContext.DECIMAL128);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "SumOfPricePerWeekDay");
		job.setJarByClass(PurchaseSalesJob.class);
		job.setMapperClass(PricePerDayMapper.class);
		job.setCombinerClass(SumOfPricePerWeekDayReducer.class);
		job.setReducerClass(SumOfPricePerWeekDayReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}