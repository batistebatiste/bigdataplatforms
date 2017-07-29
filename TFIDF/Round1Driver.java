package ecp.Lab1.Tfidf;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class Round1Driver extends Configured implements Tool {
	
	private static final String Inputpath = "input"; // folder in hdfs with input
	private static final String Outputpath = "WordFreq"; //folder in hdfs where the output will go
	
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      
	      Job job1 = new Job(getConf(), "WordFrequency in doc");
	      job1.setJarByClass(Round1Driver.class);
	      job1.setOutputKeyClass(Text.class);
	      job1.setOutputValueClass(IntWritable.class);

	      job1.setMapperClass(Round1Mapper.class);
	      job1.setReducerClass(Round1Reducer.class);
	      job1.setCombinerClass(Round1Reducer.class);

	      
	      FileInputFormat.addInputPath(job1, new Path(Inputpath));
	      FileOutputFormat.setOutputPath(job1, new Path(Outputpath));

	      return job1.waitForCompletion(true) ? 0:1;

	   }
	
	
	
	public static void main(String[] args) throws Exception {
	      int res = ToolRunner.run(new Configuration(), new Round1Driver(), args);
	      System.exit(res);
	  
	}
}
	
