package neu.termFreq;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class TermFrequency {
	public static class TMapper
	extends Mapper<LongWritable, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable(1);


		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			String line=value.toString();
			String[] words=line.split("\t");
			String artistName="";
			if(words[4].indexOf("AR")!=-1){
				if(words[8]!=null && !words[8].equals("")){
					artistName=words[8].trim();
				}

				if(words[30]!=null || !words[30].equals("")){//if Artist terms is not null in TSV file
					String trimmedLine=words[30].substring(1, words[30].length()-1);//Trim the ending square brackets [ and ]
					String[] trimmedArr=trimmedLine.split(",");//prepare and array of Artist terms so that we can pick first 6 terms
						if(trimmedArr!=null && trimmedArr.length>=6){//if trimmed array has greater than or equal to 6 elements
							for(int i=0;i<=5;i++){
								Text keyId=new Text(words[4].trim()+"_"+trimmedArr[i].trim());
								Text val=new Text("1_"+artistName);
								context.write(keyId, val);
							}
						}

				}

			}

		}
	}

	public static class TReducer
	extends Reducer<Text,Text,Text,Text> {

		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			StringBuilder names=new StringBuilder();
			for(Text i:values){
				String[] vals=i.toString().trim().split("_");
				
				sum+=Integer.parseInt(vals[0].trim());
				names.append(vals[1]).append("_");
			}
			names.deleteCharAt(names.length()-1);
			Text result=new Text(sum+"_"+names.toString().trim());
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Term Frequency-Artist Id and Single Term count");
		job.setJarByClass(TermFrequency.class);
		job.setMapperClass(TMapper.class);


		job.setReducerClass(TReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if(job.waitForCompletion(true)){
			Configuration conf1 = new Configuration();
			Job job1 = Job.getInstance(conf1, "Term Frequency-Artist Id and All terms array");
			
			job1.setMapperClass(TMapper1.class);


			job1.setReducerClass(TReducer1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job1, new Path(args[1]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			
			System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}
		
		
	}
	
	public static void run(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Term Frequency-Artist Id and All terms array");
		//job.setJarByClass(TermFrequency.class);
		job1.setMapperClass(TMapper1.class);


		job1.setReducerClass(TReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[2]));
		
		
	}



	public static class TMapper1
	extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] keyValueArray=value.toString().trim().split("\t");
			String[] keyArray=keyValueArray[0].trim().split("_");//0th element is Artist id and 1st element is Artist term
			String[] valsArray=keyValueArray[1].trim().split("_");//0th element is count of Artist terms, remaining elements are different names
			
			Text artistId=new Text(keyArray[0].trim());
			
			Text val=new Text("{"+ keyArray[1].trim()+":"+valsArray[0].trim()+"}_"+valsArray[1].trim());
			context.write(artistId, val);

		}
	}

	public static class TReducer1
	extends Reducer<Text,Text,Text,Text> {


		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			StringBuilder sb=new StringBuilder();
			
			Iterator<Text> itr=values.iterator();
			sb.append(key.toString().trim());
			String[] vals;
			if(itr.hasNext()){
				vals=itr.next().toString().trim().split("_");
				sb.append("(").append(vals[1].trim()).append("):").append("[");
				sb.append(vals[0].trim()).append(",");
			}
			
			while(itr.hasNext()){
				vals=itr.next().toString().trim().split("_");
				sb.append(vals[0].trim()).append(",");

			}
			sb.deleteCharAt(sb.length()-1);
			sb.append("]");
			context.write(null, new Text(sb.toString()));

		}
	}


}
