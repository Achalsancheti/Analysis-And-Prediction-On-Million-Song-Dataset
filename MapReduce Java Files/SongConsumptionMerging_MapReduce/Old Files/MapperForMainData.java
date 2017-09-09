package neu;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class MapperForMainData implements Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] words=line.split("\t");
		
		if(words[19].indexOf("SO")!=-1){
			Text songId=new Text(words[19].toString().trim());
			Text userAndCount=new Text(words[1].trim()+"\t"+words[2].trim());
			context.write(songId, value);
		}
		

	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		
	}
}