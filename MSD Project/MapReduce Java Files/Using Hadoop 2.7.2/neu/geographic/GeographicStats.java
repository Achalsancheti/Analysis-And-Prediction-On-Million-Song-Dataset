package neu.geographic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GeographicStats {

	public static class GeoMapper extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=line.split("\t");
			String empty="";

			if(words!=null && words.length>8){
				if(words[4].indexOf("AR")!=-1){
					if(!words[5].equals(empty) &&!words[7].equals(empty)){
						try{
							Float.parseFloat(words[5]);
							Float.parseFloat(words[7]);
							Text artistId=new Text(words[4]);
							Text latitude_longitude_ArtistName=new Text(words[5].trim()+"\t"+words[7].trim()+"\t"+words[8].trim());
							context.write(artistId, latitude_longitude_ArtistName);
						}catch(NumberFormatException ne){
							ne.printStackTrace();
						}
					}



				}
			}
		}
	}

	public static class GeoReducer	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Iterator<Text> itr=values.iterator();
			if(itr.hasNext()){
				Text value=itr.next();
				context.write(key, value);
			}


		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Geographic Stats");
		job.setJarByClass(GeographicStats.class);
		job.setMapperClass(GeoMapper.class);


		job.setReducerClass(GeoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
