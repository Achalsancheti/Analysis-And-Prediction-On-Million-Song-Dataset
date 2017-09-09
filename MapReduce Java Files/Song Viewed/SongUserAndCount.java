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
	

public class SongUserAndCount {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
			     
			
			     
			     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			       String line = value.toString();
			       String[] words=line.split("\t");
                               try{
                               if(words.length>1){
			       Text songId=new Text(words[1].toString().trim());
			       Text userAndCount=new Text(words[0].trim()+"_"+words[2].trim());
			       output.collect(songId,userAndCount);

			     }
                               }catch(NumberFormatException e){
	    		   //
	    	   }
                             }


				
			   }
			
			   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
			     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			       int sumOfUsers = 0;
			       int sumOfPlayCount=0;
			      try{
			       while (values.hasNext()) {
			    	   
			    	   sumOfUsers += 1;
			    	   String value=values.next().toString();
			    	  
			    	   String[] words=value.split("_");
			    	   if(words.length<2){
			    		   continue;
			    	   }
			    	   sumOfPlayCount+=Integer.parseInt(words[1].trim());
			       }
                              }catch(NumberFormatException ne){
                              }
                               
			       String outPutValue=Integer.toString(sumOfUsers)+"\t"+Integer.toString(sumOfPlayCount);
			      
			       output.collect(key, new Text(outPutValue));
			     }
			   }
			
			   public static void main(String[] args) throws Exception {
			     JobConf conf = new JobConf(SongUserAndCount.class);
			     conf.setJobName("Song Count");
			
			     conf.setOutputKeyClass(Text.class);
			     conf.setOutputValueClass(Text.class);
			
			     conf.setMapperClass(Map.class);
			     
			     conf.setReducerClass(Reduce.class);
			
			     conf.setInputFormat(TextInputFormat.class);
			     conf.setOutputFormat(TextOutputFormat.class);
			
			     FileInputFormat.setInputPaths(conf, new Path("C:/cygwin64/usr/local/hadoop-0.20.2/input6666"));
			     FileOutputFormat.setOutputPath(conf, new Path("C:/cygwin64/usr/local/hadoop-0.20.2/output5555"));
			
			     JobClient.runJob(conf);
			   }
}
