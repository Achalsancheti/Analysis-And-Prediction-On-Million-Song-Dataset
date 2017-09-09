package neu.mostDenseSong;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




//Most Dense song
public class MostDenseSong {
	public static class MDMapper
	extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String beats=words[10].trim();
			String duration=words[12].trim();
			try{
				Double beatsDouble=Double.parseDouble(beats);
				Double durationDouble=Double.parseDouble(duration);
				if(beatsDouble>0 && durationDouble>0){
					if(words[19].trim().indexOf("SO")!=-1){
						if(!words[22].equals("")){
							DoubleWritable density=new DoubleWritable(beatsDouble/durationDouble);
							Text songIdAndName=new Text(words[19].trim()+"("+words[22].trim()+")");
							context.write(density, songIdAndName);
						}
					}
				}
			}catch(NumberFormatException ne){
				ne.printStackTrace();
			}
		}
	}

	public static class MDReducer
	extends Reducer<DoubleWritable, Text,DoubleWritable,Text> {
		public void reduce(DoubleWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
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
		job.setJarByClass(MostDenseSong.class);
		job.setMapperClass(MDMapper.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MDReducer.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
