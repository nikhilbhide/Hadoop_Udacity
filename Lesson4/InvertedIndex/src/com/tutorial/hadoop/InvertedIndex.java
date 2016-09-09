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
 * Implements mappers and reducers for inverted index
 * 
 * @author nikhil.bhide
 * @version 1.0
 * @since 1.0
 */
public class InvertedIndex {
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			if(tokens.length>=5) {
				try {
					String body = tokens[4];
					String nodeId =  tokens[0];
					String [] bodyTokens = body.split("\\.|,|\\?|:|;|\"|\\(|\\)|<|>|\\[|\\]|#|$|=|-|/|\\s");
					for(String bodyToken:bodyTokens) {
						if(bodyToken.toLowerCase().equals("fantastic"))
							context.write(new Text(bodyToken.toLowerCase()), new Text(new LongWritable(1).toString()));
						else if (bodyToken.toLowerCase().equals("fantastically")) 
							context.write(new Text(bodyToken.toLowerCase()), new Text(nodeId));
						//context.write(new Text(value), new Text(String.valueOf(nodeId).concat(":").concat(new LongWritable(1).toString())));
					}					
				}				
				catch(Exception e) {
					throw e;
				}
			}
		}
	}

	/**
	 * Reducer class for all inverted index
	 *
	 */
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
		/* (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(key.toString().equals("fantastic"))
				context.write(key, getSum(values));
			else if (key.toString().equals("fantastically")) {
				context.write(key, getNodeList(values));
			}
		}

		private Text getSum(Iterable<Text> values) {
			long sum = 0;
			for (Text val : values) {
				sum+= Long.parseLong(val.toString());			
			}
			return new Text(String.valueOf(sum));
		}
	}

	/**
	 * Calculates sum of all values by iterating through list of values of type {@link Text}
	 * 
	 * @param values The list of values of type {@link Text}ata
	 * @return Concatenated list of node ids and count for word occurence
	 */
	private static Text getNodeList(Iterable<Text> values) {
		String nodeIds = "";
		for (Text val : values) {
			nodeIds = nodeIds.concat(",").concat(val.toString());
		}
		return new Text(nodeIds);			
	}

	/**
	 * Main function for executing hadoop jon
	 * @param args The first argument should be input file path and second one should be output file path
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setCombinerClass(InvertedIndexReducer.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}