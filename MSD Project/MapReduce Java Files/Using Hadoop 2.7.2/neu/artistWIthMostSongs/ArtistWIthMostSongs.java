package neu.artistWIthMostSongs;

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




public class ArtistWIthMostSongs {
	public static class MSMapper
	extends Mapper<LongWritable, Text, Text, Text>{




		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String artistId=words[4].trim();
			String artistName;
			if(artistId.indexOf("AR")!=-1){
				if(words[8].trim().equals("")){
					artistName="NA";
				}else{
					artistName=words[8].trim();
				}
				
				context.write(new Text(artistId), new Text(artistName));
			}


		}
	}

	public static class MSReducer
	extends Reducer<Text,Text,IntWritable,Text> {



		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Iterator<Text> itr=values.iterator();
			int sum=0;
			Text name=null;
			if(itr.hasNext())
			{
				sum++;
				name=itr.next();
			}
			while(itr.hasNext()){
				itr.next();
				sum++;
			}
			Text keyOut=null;
			IntWritable sumValue=new IntWritable(sum);
			if(name==null){
				keyOut=new Text(key.toString()+"(Name not Available)");

			}else{
				keyOut=new Text(key.toString()+"("+name.toString()+")");
			}
		
			context.write(sumValue, keyOut);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(ArtistWIthMostSongs.class);
		job.setMapperClass(MSMapper.class);


		job.setReducerClass(MSReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
