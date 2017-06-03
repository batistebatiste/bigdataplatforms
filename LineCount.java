package ecp.tuto1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;


import java.io.*;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


public class LineCount {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path(args[0]);
		
		//Open the file
		int lignes = 0;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			String tree =new String();
			String year = new String();
			String height = new String();
			while (line !=null){
				lignes += 1;
				// Process of the current line
				//System.out.println(line);
				// go to the next line
				String provisoire []=line.toString().split(";");
				tree = provisoire[2];
				year = provisoire[5];
				height =provisoire[6];
				line = br.readLine();
				System.out.println( "Tree: " + tree + ", Year: " + year + ", Height: "+ height); 

			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}
		
		System.out.println("This document has " + lignes + " lines");
		
		
	}

}