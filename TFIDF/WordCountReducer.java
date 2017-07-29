package ecp.Lab1.Tfidf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.*;

//Reducer of wordcount round 
// key is the key of the mapper, values are the values from mapping

public  class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	public WordCountReducer(){
	}
	
	protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
		int sum = 0;
   
		Map<String, Integer> temp = new HashMap<String, Integer>();
		for (Text val : values) {
			String[] wordCounter = val.toString().split("=");
			temp.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
			sum += Integer.parseInt(val.toString().split("=")[1]);
		}
		for (String wordKey : temp.keySet()) {
			context.write(new Text(wordKey + "@" + key.toString()), new Text(temp.get(wordKey) + "/"
					+ sum));
		}
	}
}