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


public class WordCountDriver extends Configured implements Tool {
	
	 private static final String Output = "WordCount";
	 private static final String Input = "WordFreq";
	
		 
	   public int run(String[] args) throws Exception {
		 	      
	      Job job = new Job(getConf(), "Term count");
	      job.setJarByClass(WordCountDriver.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);

	      job.setMapperClass(WordCountMapper.class);
	      job.setReducerClass(WordCountReducer.class);

	      
	      FileInputFormat.addInputPath(job, new Path(Input));
	      FileOutputFormat.setOutputPath(job, new Path(Output));

	      return job.waitForCompletion(true)? 0:1;
	    
	   }



public static void main(String[] args) throws Exception {
    System.out.println(Arrays.toString(args));
    int res = ToolRunner.run(new Configuration(), new WordCountDriver(), args);
    System.exit(res);
 }
}

	
