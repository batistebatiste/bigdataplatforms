package ecp.Lab1.Tfidf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//Reducer of round 1 

public  class Round1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public Round1Reducer() {
    }
	
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
      sum += val.get();
   }
   context.write(key, new IntWritable(sum));
}
}

//output is smthg like <"word@filename", 4>






