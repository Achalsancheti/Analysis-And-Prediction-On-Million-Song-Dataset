package neu.beatsPerYear;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class BeatsPerYearAverageMapReduce {
	public static class BAMapper
	extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String yearString=words[25].trim();
			try{
				int year =Integer.parseInt(yearString);
				if(year>0){
					int beats=Integer.parseInt(words[10].trim());
					if(beats>0){
						System.out.println(beats);
						DoubleWritable beatsValue=new DoubleWritable(beats);
						context.write(new IntWritable(year), beatsValue);
					}
				}
			}catch(NumberFormatException ne){
				ne.printStackTrace();

			}
		}
	}

	public static class BAReducer
	extends Reducer<IntWritable, DoubleWritable,IntWritable,DoubleWritable> {
		public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
			Iterator<DoubleWritable> itr=values.iterator();
			double average=0;
			int count=0;
			while(itr.hasNext()){
				average+=itr.next().get();
				count++;
			}
			DoubleWritable averagePerYear=new DoubleWritable(average/count);
			context.write(key,averagePerYear);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Beats Per Year");
		job.setJarByClass(BeatsPerYearAverageMapReduce.class);
		job.setMapperClass(BAMapper.class);


		job.setReducerClass(BAReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
