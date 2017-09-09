package neu.songCountVerification;

import java.io.IOException;
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


public class SoungCountVerification {

	public static class SongMapper
	extends Mapper<LongWritable, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);


		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			if(words!=null && words.length>1){
				if(words[19].indexOf("SO")!=-1){
					context.write(new Text(""), one);
					System.out.println(words[19]);
				}
			}
		}
	}

	public static class SongReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable(1);

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable i:values){
				sum+=i.get();
			}
			result.set(sum);
			context.write(new Text("Total Songs"), result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(SoungCountVerification.class);
		job.setMapperClass(SongMapper.class);
		

		job.setReducerClass(SongReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
