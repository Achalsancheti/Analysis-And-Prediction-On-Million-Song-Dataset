package neu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class MapForSongConsumption  extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {

		String line = value.toString();
		String[] words=line.split("\t");
		
		Text songId=new Text(words[0].toString().trim());
		Text userAndCount=new Text(words[1].trim()+"\t"+words[2].trim());
		context.write(songId, userAndCount);
	}


}
