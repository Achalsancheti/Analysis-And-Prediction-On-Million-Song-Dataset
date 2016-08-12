package neu.loudestSong;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class LoudestSongMapReduce {
	public static class LSMapper
	extends Mapper<LongWritable, Text, FloatWritable, Text>{

		


		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String songId=words[19].trim();
			if(songId.indexOf("SO")!=-1){
				try{
					float k=Float.parseFloat(words[15].trim());
					if(k<0){
						System.out.println(k);
						FloatWritable floatKey=new FloatWritable(k);
						context.write(floatKey, new Text(songId));
					}
					
				}catch(NumberFormatException ne){
					//ne.printStackTrace();
					
				}
				
			}
			
			
		}
	}

	public static class LSReducer
	extends Reducer<FloatWritable,Text,FloatWritable,Text> {

		

		public void reduce(FloatWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Iterator<Text> itr=values.iterator();
			StringBuilder sb=new StringBuilder();
			sb.append("[");
			while(itr.hasNext()){
				sb.append(itr.next().toString().trim()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			sb.append("]");
			context.write(key,new Text(sb.toString().toString()) );
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(LoudestSongMapReduce.class);
		job.setMapperClass(LSMapper.class);
		

		job.setReducerClass(LSReducer.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
