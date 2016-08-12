/*
This is a Chained Map Reduce job to calculate the number of Artists who made debut per year.
Chaining is done in two steps:

Step 1:
Emit (Artist Id/Debut year) pair from ArtistPerYear.java MapReduce.

The output of step one is fed as input to YearToArtist.java MapReduce.

Step 2
Emit (Year/Number of Artists) pair from YearToArtist.java MapReduce.

The output from step 2 is the desired result.


*/

package neu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ArtistPerYear {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     
		
	     
	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       String line = value.toString();
	       String[] words=line.split("\t");
	       String artistId=words[4].toString().trim();
	       if(artistId.indexOf("AR")!=-1){
	    	   try{
	    		   Integer.parseInt(words[25]);
	    		   if(words[25].length()==4){
	    			   output.collect(new Text(artistId),new Text(words[25].trim()));
	    		   }
	    		   
	    	   }catch(NumberFormatException e){
	    		   //
	    	   }
	    	   
	       }
	
	      }
	}
	
	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	
	       int debutYear=Integer.MAX_VALUE;
	      
	       
	       while (values.hasNext()) {
	    	   int value=Integer.parseInt(values.next().toString());
	    	   debutYear=value<debutYear?value:debutYear;
	    	  
	       }
	       
	       output.collect(key, new Text(Integer.toString(debutYear)));
	     }
	   }
	
	   public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(ArtistPerYear.class);
	     conf.setJobName("Artist and Year");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	     run(args);
	     
	   }
	   
	   public static void run(String[] args) throws Exception{
		   JobConf conf1 = new JobConf(YearToArtist.class);
		     conf1.setJobName("Artist Count");
		
		     conf1.setOutputKeyClass(Text.class);
		     conf1.setOutputValueClass(Text.class);
		
		     conf1.setMapperClass(YearToArtist.Map1.class);
		     
		     conf1.setReducerClass(YearToArtist.Reduce1.class);
		
		     conf1.setInputFormat(TextInputFormat.class);
		     conf1.setOutputFormat(TextOutputFormat.class);
		     FileInputFormat.setInputPaths(conf1, new Path(args[1]+"/part-00000"));
		     FileOutputFormat.setOutputPath(conf1, new Path(args[2]));
		     JobClient.runJob(conf1);
	   }
	   
}

