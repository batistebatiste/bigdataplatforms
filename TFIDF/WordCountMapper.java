package ecp.Lab1.Tfidf;

// Mapper of the first round. 
// input (docname, content)
// output ((word, docname), 1) 

import java.io.IOException; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    private Text fileName = new Text();
    
    public WordCountMapper(){
         }
    
    // key is which line, value is the line from file 
    
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String[] full = value.toString().split("/t"); //tab delimited
    	String[] wordanddocument = full[0].split("@"); // see previous
    	// so we have as output the word the file and the count 
    	context.write(new Text(wordanddocument[1]), new Text(wordanddocument[0] + "=" + full[1] ));
    	
    
    }
 }