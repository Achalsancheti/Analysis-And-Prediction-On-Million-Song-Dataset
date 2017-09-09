package neu;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;


public class CombinerDriver {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(neu.CombinerDriver.class);

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
//		FileInputFormat.addInputPath(conf,new Path("src"));
		FileOutputFormat.setOutputPath(conf,new Path("/Users/saurabh/Documents/BigDataProject/SongConsumptionMerge/output"));
	
		MultipleInputs.addInputPath(conf, new Path("/Users/saurabh/Documents/BigDataProject/SongConsumptionMerge/consumption_input/"),TextInputFormat.class,neu.MapForSongConsumption.class);
		//MultipleInputs.addInputPath(conf, new Path("/Users/saurabh/Documents/BigDataProject/SongConsumptionMerge/consumption_input/"), TextInputFormat.class);
		MultipleInputs.addInputPath(conf, new Path("/Users/saurabh/Documents/BigDataProject/MergingInputs/output/"), TextInputFormat.class, neu.MapperForMainData.class);

		// TODO: specify a mapper
		//conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);

		// TODO: specify a reducer
		conf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
