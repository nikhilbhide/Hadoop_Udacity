package com.tutorial.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Implements mappers and reducers for analyzing web access log data
 * 
 * @author nikhil.bhide
 * @version 1.0
 * @since 1.0
 */
public class AccessLogs {
	public static class AccessLogsTotalHitsPerFileMapper extends Mapper<Object, Text, Text, LongWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\s");
			if(tokens.length>=7 && tokens[6].equals("/assets/js/the-associates.js")) {
				try {
					String fileName = tokens[6];
					context.write(new Text(fileName), new LongWritable(1));
				}
				catch(Exception e) {
					throw e;
				}
			}
		}
	}
	
	/**
	 * Mapper class for popular file quiz
	 * Lesson - 3
	 * Quiz - Popular file
	 * Once output is caluclated, copy the file to local disk and apply sort on column 2
	 * 
	 */
	public static class AccessLogsPopularFileMapper extends Mapper<Object, Text, Text, LongWritable> {
		
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\s");
			if(tokens.length>=7) {
				try {
					String fileName = tokens[6];
					if(fileName.startsWith("http://www.the-associates.co.uk")) {
						fileName = fileName.split("http://www.the-associates.co.uk")[1];
					}
					context.write(new Text(fileName), new LongWritable(1));
				}
				catch(Exception e) {
					throw e; 
				}
			}
		}
	}
	
	/**
	 * Mapper class for total number of hits from IP
	 * Lesson - 3
	 * Quiz - total hits from ip
	 * 
	 */
	public static class AccessLogsTotalHitsPerIPMapper extends Mapper<Object, Text, Text, LongWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\\s");
			if(tokens.length>=7) {
				try {
					String fileName = tokens[0];
					context.write(new Text(fileName), new LongWritable(1));
				}
				catch(Exception e) {
					throw e; 
				}
			}
		}
	}
	
	/**
	 * Common reducer class for all mappers
	 *
	 */
	public static class AccessLogsTotalHitsReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, getSum(values));
		}
	}

	/**
	 * Calculates sum of all values by iterating through list of values of type {@link LongWritable}
	 * 
	 * @param values The list of values of type {@link LongWritable}
	 * @return sum The sum of all values
	 */
	private static LongWritable getSum(Iterable<LongWritable> values) {
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
		}
		return new LongWritable(sum);			
	}

	/**
	 * Main function for executing hadoop jon
	 * @param args The first argument should be input file path and second one should be output file path
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AccessLogsTotalHitsPerPage");
		//change to different jobs instances as required
		//Job job = Job.getInstance(conf, "AccessLogsPopularFile");
		//Job job = Job.getInstance(conf, "AccessLogsTotalHitsPerIP");
		job.setJarByClass(AccessLogs.class);
		job.setMapperClass(AccessLogsPopularFileMapper.class);
		//change to different mappers as required
		//job.setMapperClass(AccessLogsTotalHitsPerIPMapper.class);
		//job.setMapperClass(AccessLogsTotalHitsPerFileMapper.class);
		job.setCombinerClass(AccessLogsTotalHitsReducer.class);
		job.setReducerClass(AccessLogsTotalHitsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}