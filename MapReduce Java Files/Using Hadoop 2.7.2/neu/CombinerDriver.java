package neu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CombinerDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception{


		ToolRunner.run(new Configuration(), new CombinerDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(CombinerDriver.class);
		MultipleInputs.addInputPath(job, new Path("/Users/saurabh/Documents/BigDataProject/SongConsumptionMerge/consumption_input/"),TextInputFormat.class, neu.MapForSongConsumption.class);
		MultipleInputs.addInputPath(job, new Path("/Users/saurabh/Documents/BigDataProject/MergingInputs/output/"),TextInputFormat.class, MapperForMainData.class);

		FileOutputFormat.setOutputPath(job, new Path("/Users/saurabh/Documents/BigDataProject/SongConsumptionMerge/output"));
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
