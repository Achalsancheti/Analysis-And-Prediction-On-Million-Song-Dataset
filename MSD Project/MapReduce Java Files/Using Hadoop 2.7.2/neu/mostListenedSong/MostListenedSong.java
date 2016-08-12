package neu.mostListenedSong;

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




public class MostListenedSong {
	public static class MLSMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>{




		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String userCount=words[35].trim();

			try{
				Integer count=Integer.parseInt(userCount);
				if(count>0){
					if(words[19].trim().indexOf("SO")!=-1){

						if(!words[22].equals("")){
							Text songIdAndName=new Text(words[19].trim()+"("+words[22].trim()+")");
							context.write(new IntWritable(count), songIdAndName);
						}

					}

				}


			}catch(NumberFormatException ne){
				//ne.printStackTrace();
			}

		}


	}


	public static class MLSReducer
	extends Reducer<IntWritable, Text,IntWritable,Text> {



		public void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
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
		Job job = Job.getInstance(conf, "MostListenedSong");
		job.setJarByClass(MostListenedSong.class);
		job.setMapperClass(MLSMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MLSReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
