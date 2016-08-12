package neu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class YearToArtist {
	
		public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		     
			
		     
		     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		       String line = value.toString();
		       String[] words=line.split("\t");
		       System.out.println(line);
		       String year=words[1].toString().trim();
		       if(!year.equals("0")){
		    	   output.collect(new Text(year),new Text(Integer.toString(1)));
		       }
		       
		       
		
		      }
		}
		
		   public static class Reduce1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
		    	 int sumOfArtist=0;
			      
			       while (values.hasNext()) {
			    	   sumOfArtist+=1;
			    	   values.next();
			       }
			      
			      
			       output.collect(key, new Text(Integer.toString(sumOfArtist)));
			     }
		     }
		   
	}

