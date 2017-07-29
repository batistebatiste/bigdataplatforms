package ecp.Lab1.Tfidf;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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


public class Round3Driver extends Configured implements Tool {
	
	private static final String Inputpath = "WordCount"; // folder in hdfs with input
	private static final String Outputpath = "TFIDF"; //folder in hdfs where the output will go
	
	public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      
	      Job job3 = new Job(getConf(), "last job");
	      job3.setJarByClass(Round3Driver.class);
	      job3.setOutputKeyClass(Text.class);
	      job3.setOutputValueClass(Text.class);

	      job3.setMapperClass(Round3Mapper.class);
	      job3.setReducerClass(Round3Reducer.class);

	      FileInputFormat.addInputPath(job3, new Path(Inputpath));
	      FileOutputFormat.setOutputPath(job3, new Path(Outputpath));

	   // get n number of documents in folder
	        Path inputPath = new Path("input");
	        FileSystem fs = inputPath.getFileSystem(getConf());
	        FileStatus[] stat = fs.listStatus(inputPath);
	 
	        job3.setJobName(String.valueOf(stat.length));
	      
	      
	      job3.waitForCompletion(true);
	      return 0;
	   }
	
	
	
	public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int res = ToolRunner.run(new Configuration(), new Round3Driver(), args);
	      System.exit(res);
	  
	}
}
	
