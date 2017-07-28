package ecp.Lab1.WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
/*
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	@Override
	 protected void reduce(Text key, Iterable<LongWritable> values, Context context)
	      throws IOException, InterruptedException {

		long sum = 0;
		for (LongWritable value : values){
			sum=sum+value.get();
		}
		
		context.write(key, new LongWritable(sum));
	    
	  }

}	
*/



public  class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
@Override
public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
      sum += val.get();
   }
   context.write(key, new IntWritable(sum));
}
}
