package com.tutorial.hadoop;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PurchaseSalesByCategory {
	public static class StoreMapper
	extends Mapper<Object, Text, Text, Text>{

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

	public static class IntSumReducer
	extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			context.write(key, new Text(getMax(values).toString()));
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

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StoreMapper");
		job.setJarByClass(PurchaseSalesByCategory.class);
		job.setMapperClass(StoreMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}