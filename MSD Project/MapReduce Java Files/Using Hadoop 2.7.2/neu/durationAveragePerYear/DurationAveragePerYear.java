package neu.durationAveragePerYear;

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




public class DurationAveragePerYear {
	public static class DAMapper
	extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{




	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String yearString=words[25].trim();
			try{
				int year =Integer.parseInt(yearString);
				if(year>0){
					double k=Double.parseDouble(words[12].trim());
					if(k>0){
						System.out.println(k);
						DoubleWritable duration=new DoubleWritable(k);
						context.write(new IntWritable(year), duration);
					}
				}
			}catch(NumberFormatException ne){
				ne.printStackTrace();
			}
		}
	}

	public static class DAReducer
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
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(DurationAveragePerYear.class);
		job.setMapperClass(DAMapper.class);
		job.setReducerClass(DAReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
