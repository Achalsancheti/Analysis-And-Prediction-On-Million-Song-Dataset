package neu.ArtistsPerMBTag;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ArtistsPerMBTags {
	public static class APMBTMapper	extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String mbtags=words[27].trim();
			if(mbtags.indexOf("[")!=-1){
				mbtags=mbtags.substring(1, mbtags.length()-1);
				String[] mbtagsArray=mbtags.split(",");
				if(mbtagsArray.length>0){
					if(words[4].indexOf("AR")!=-1){
						String artistName=words[4].trim();
						for(String mbtag:mbtagsArray){
							context.write(new Text(mbtag.trim()+"_"+artistName), new IntWritable(1));
						}
					}
				}
			}
		}
	}

	public static class APMBTReducer extends Reducer<Text, IntWritable,Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> itr=values.iterator();
			int sum=0;
			while(itr.hasNext())
			{
				sum+=itr.next().get();

			}
			IntWritable sumValue=new IntWritable(sum);
			context.write(key, sumValue);
		}
	}
	public static class APMBTMapper1
	extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String[] keyArray=words[0].trim().split("_");
			context.write(new Text(keyArray[0].trim()), new IntWritable(1));
		}
	}

	public static class APMBTReducer1	extends Reducer<Text, IntWritable,Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			Iterator<IntWritable> itr=values.iterator();
			int sum=0;
			while(itr.hasNext())
			{
				sum+=itr.next().get();
			}
			IntWritable sumValue=new IntWritable(sum);
			context.write(key, sumValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ArtistsPerMBTags");
		job.setJarByClass(ArtistsPerMBTags.class);
		job.setMapperClass(APMBTMapper.class);


		job.setReducerClass(APMBTReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);

		Configuration conf1 = new Configuration();
		if(job.waitForCompletion(true)){

			Job job1 = Job.getInstance(conf1, "ArtistsPerMBTags");
			job1.setJarByClass(ArtistsPerMBTags.class);
			job1.setMapperClass(APMBTMapper1.class);


			job1.setReducerClass(APMBTReducer1.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(IntWritable.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}

	}
	

}
