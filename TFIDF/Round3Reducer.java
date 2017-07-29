package ecp.Lab1.Tfidf;

import java.io.IOException;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 


//Reducer of round 3 

public  class Round3Reducer extends Reducer<Text, Text, Text, Text>  {
 
    private static final DecimalFormat DF = new DecimalFormat("###.##");
    public Round3Reducer() {
    }


    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Get nb of docs in corpus 
        int N = Integer.parseInt(context.getJobName());
        // frequency of the word
        int docswherekey = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split("=");
            docswherekey++;
            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
        }
        for (String document : tempFrequencies.keySet()) {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");
            //term frequency
            double tf = Double.valueOf(Double.valueOf(wordFrequenceAndTotalWords[0])
                    / Double.valueOf(wordFrequenceAndTotalWords[1]));
 
            // Inverse document frequency  
            double idf = (double) N / (double) docswherekey;
 
            
            double tfIdf = N == docswherekey ?
                    tf : tf * Math.log10(idf);
 
            context.write(new Text(key + "@" + document), new Text("[" + docswherekey + "/"
                    + N + " , " + wordFrequenceAndTotalWords[0] + "/"
                    + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
        }
    }
}




