package ecp.tuto1;

import org.apache.hadoop.conf.Configuration;
import java.io.*;
import org.apache.hadoop.fs.*;


public class Question28 {

	public static void main(String[] args) throws IOException {
		
		
		Path data = new Path("/home/cloudera/workspace/Batiste/input/isd-history.txt");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(data);
		int i =0;
		
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			
			String name=new String();
			String country=new String();
			String elevation=new String();
			String usaf=new String();
			
			
			
			while (line !=null){
				i=i+1;
				// Process of the current line
				//System.out.println(line);
				line= br.readLine();
				// go to the next line				
				//line = br.readLine();
				
					if(i>=22){
						//we use the string.substring method to extract the info 
						name=line.substring(13, (13+29));
						country=line.substring(43,45);
						elevation=line.substring(74,(74+7));
						usaf=line.substring(0, 6);
						
						System.out.println(name+" : "+ country+ " : "+elevation);
						
						line=br.readLine();
				}
			}
			
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}
				
		
	}
}
	
