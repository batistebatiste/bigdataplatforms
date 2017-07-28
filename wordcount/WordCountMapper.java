package ecp.Lab1.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


/*
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	// Écriture de la fonction map
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
	    	{
			//read file line by line
			String line = value.toString();
			//split line into words
			String[] words = line.split(" ");
			//assign count(1) to each word 
			for (String word : words){
				context.write(new Text(word),  new LongWritable(1));
			}
	    	}
	        	
	    }

*/

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
         for (String token: value.toString().split("\\s+")) {
            word.set(token);
            context.write(word, ONE);
         }
      }
   }
 


