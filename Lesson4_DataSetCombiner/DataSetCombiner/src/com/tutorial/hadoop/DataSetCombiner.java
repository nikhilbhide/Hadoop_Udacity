package com.tutorial.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DataSetCombiner {
	public static class ForumNodeMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			System.out.println("token length is" + tokens.length);
			if(tokens.length>=4) {
				try {
					String id =  tokens[0];
					String title = tokens[1];
					String tagNames = tokens[2];
					String authorId = tokens[3];
					context.write(new Text(authorId), new Text("ForumNode_File".concat(id).concat(",").concat(title).concat(",").concat(tagNames)));
				}
				catch(Exception e) {
					System.out.println("Exception");
				}
			}
		}
	}

	public static class ForumUsersMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String tokens[] = value.toString().split("\t");
			System.out.println("token length is" + tokens.length);
			if(tokens.length>=1) {
				try {
					String authorId =  tokens[0];
					String reputation = tokens[1];
					String gold = tokens[2];
					String silver = tokens[3];
					context.write(new Text(authorId), new Text("ForumUser_File".concat(reputation).concat(",").concat(gold).concat(",").concat(silver)));
				}
				catch(Exception e) {
					System.out.println("Exception");
				}
			}
		}
	}

	public static class ForumDataReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> nodeForumData = new ArrayList();
			String nodeUserData = "Author ID NOT AVAILABLE";
			for(Text value:values) {
				String forumData = value.toString();
				if(forumData.startsWith("ForumNode_File")) {
					forumData = forumData.replace("ForumNode_File","");
					nodeForumData.add(forumData);
				}
				else if(forumData.startsWith("ForumUser_File")) {
					forumData = forumData.replace("ForumUser_File","");
					nodeUserData = "USER OF THE POST IS ".concat(forumData);
				}
			}

			for(String data:nodeForumData)
				context.write(key, new Text(data.concat(",").concat(nodeUserData)));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DataSetCombiner");
		job.setJarByClass(DataSetCombiner.class);
		job.setReducerClass(ForumDataReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ForumNodeMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ForumUsersMapper.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}