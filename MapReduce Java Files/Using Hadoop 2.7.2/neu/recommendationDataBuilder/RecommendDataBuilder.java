package neu.recommendationDataBuilder;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class RecommendDataBuilder {

	public static class RecMapper extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			Text userId=new Text(words[0].trim());
			Text songAndCound=new Text("{"+words[1].trim()+","+words[2].trim()+"}");
			context.write(userId, songAndCound);
		}
	}

	public static class RecReducer	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			StringBuilder sb=new StringBuilder();

			Iterator<Text> itr=values.iterator();
			sb.append(key.toString().trim()).append(":");
			sb.append("[");
			while(itr.hasNext()){

				sb.append(itr.next().toString().trim()).append(",");

			}
			sb.deleteCharAt(sb.length()-1);
			sb.append("]");
			context.write(null, new Text(sb.toString()));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Base data builder for Recommendation");
		job.setJarByClass(RecommendDataBuilder.class);
		job.setMapperClass(RecMapper.class);
		
		conf.set("mapred.textoutputformat.separator",":");
		conf.set("mapreduce.output.textoutputformat.separator", ":");
        conf.set("mapreduce.output.key.field.separator", ":");
        conf.set("mapred.textoutputformat.separatorText", ":"); 
        
        
		job.setReducerClass(RecReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
