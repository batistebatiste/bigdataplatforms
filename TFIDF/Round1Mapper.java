package ecp.Lab1.Tfidf;

/// Mapper of the first round.
// input (docname, content)
// output ((word, docname), 1) 

import java.io.IOException; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
 

public class Round1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
   public Round1Mapper(){
   }
	
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // key is which line we're on, value is the ine from the file
	   
	   // from www.vogella.com/tutorials/JavaRegularExpressions/article.html
	   Pattern p = Pattern.compile("\\w+");
       Matcher m = p.matcher(value.toString()); // transform to string
       
       // Get name of  file
       String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

       StringBuilder valueBuilder = new StringBuilder();
       while (m.find()) {
           String matchedKey = m.group().toLowerCase();
           if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                  || matchedKey.contains("_")) {
               continue;
           }
           valueBuilder.append(matchedKey);
           valueBuilder.append("@");
           valueBuilder.append(fileName);
           // Emit key value pair <k,v>
           context.write(new Text(valueBuilder.toString()), new IntWritable(1));
           // <word@filename,1>
       }
   }
}