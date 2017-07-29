package ecp.Lab1.Tfidf;

/// Mapper of the third round.
 

import java.io.IOException; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.*;



public class Round3Mapper extends Mapper<LongWritable, Text, Text, Text> {
   public Round3Mapper(){
   }
	
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String[] full = value.toString().split("\t");
       String[] wordanddocument = full[0].split("@");
       context.write(new Text(wordanddocument[0]), new Text(wordanddocument[1] + "=" + full[1]));

   }
}