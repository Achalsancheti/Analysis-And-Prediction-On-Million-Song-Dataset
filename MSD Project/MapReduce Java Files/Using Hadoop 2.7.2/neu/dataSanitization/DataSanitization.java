package neu.dataSanitization;

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


/*
analysis_sample_rate	0
artist_7did				1
artist_familiarity		2
artist_hotness			3
artist_id				4
artist_latitude			5
artist_location			6
artist_longitude		7
artist_name				8
audio_md5				9
number_of_beats			10
danceability			11
duration				12
end_of_fade_in			13
energy					14
loudness				15
release					16
release_7did			17
hotness					18
songid					19
start_of_fade_out		20
tempo					21
title					22
trackid					23
track_7did				24
year					25
artist_mbid				26
artist_mbtags			27
artist_mbcount			28
artist_playmeid			29
artist_terms			30
artist_termfrequency	31
artist_termweight		32
similar_artists			33
user_count				34
num_played				35

 */
public class DataSanitization {


	public static class SMapper extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line=value.toString();
			String[] words=line.split("\t");
			//System.out.println(words.length);
			if(words.length==34){
				try{

					String analysis_sample_rate=words[0].trim();
					double asr=Double.parseDouble(analysis_sample_rate);

					if(asr!=22050){
						words[0]="22050";
					}


					String artist_7did=words[1].trim();
					if(artist_7did.equals("")){
						words[1]="NA";

					}

					String artist_familiarity=words[2].trim();
					Float.parseFloat(artist_familiarity);

					String artist_hotness=words[3].trim();
					Float.parseFloat(artist_hotness);

					String artist_id=words[4].trim();
					if(artist_id.indexOf("AR")==-1){
						words[4]="NA";
					}

					
					String artist_latitude=words[5].trim();
					
					
					
					if(artist_latitude.equals("")){
						words[5]="0";
					}else{
						Float.parseFloat(artist_latitude);
					}
					//System.out.println("artist_latitude"+artist_latitude);

					String artist_location=words[6].trim();
					if(artist_location.equals("")){
						words[6]="NA";
					}

					String artist_longitude=words[7].trim();
					
					if(artist_longitude.equals("")){
						words[7]="0";
					}else{
						Float.parseFloat(artist_longitude);
					}
					//System.out.println("artist_longitude"+artist_longitude);

					String artist_name=words[8].trim();
					if(artist_name.equals("")){
						words[8]="NA";
					}

					String audio_md5=words[9].trim();
					if(audio_md5.equals("")){
						words[9]="NA";
					}


					String number_of_beats=words[10].trim();
					if(number_of_beats.equals("")){
						words[10]="0";
					}else{
						Integer.parseInt(number_of_beats);
					}


					String danceability=words[11].trim();
					if(danceability.equals("")){
						words[11]="0";
					}else{
						Double.parseDouble(danceability);
					}

					String duration=words[12].trim();
					if(duration.equals("")){
						words[12]="0";
					}else{
						Float.parseFloat(duration);
					}

					String end_of_fade_in=words[13].trim();
					if(end_of_fade_in.equals("")){
						words[13]="0";
					}else{
						Float.parseFloat(end_of_fade_in);
					}

					String energy=words[14].trim();
					if(energy.equals("")){
						words[14]="0";
					}else{
						Double.parseDouble(energy);
					}


					String loudness=words[15].trim();
					if(loudness.equals("")){
						words[15]="0";
					}else{
						Float.parseFloat(loudness);
					}
					
					

					String release=words[16].trim();
					if(release.equals("")){
						words[16]="NA";
					}
					
					String release_7did=words[17].trim();
					if(release_7did.equals("")){
						words[17]="0";
					}else{
						Integer.parseInt(release_7did);
					}
					
					
					String hotness=words[18].trim();
					if(hotness.equals("")){
						words[18]="0";
					}else{
						Float.parseFloat(hotness);
					}
					
					
					String songid=words[19].trim();
					if(songid.indexOf("SO")==-1){
						words[19]="NA";
					}
					
					String start_of_fade_out=words[20].trim();
					if(start_of_fade_out.equals("")){
						words[20]="0";
					}else{
						Float.parseFloat(start_of_fade_out);
					}
					
					String tempo=words[21].trim();
					if(tempo.equals("")){
						words[21]="0";
					}else{
						Float.parseFloat(tempo);
					}
					
					String title=words[22].trim();
					if(title.equals("")){
						words[22]="NA";
					}
					
					String trackid=words[23].trim();
					if(trackid.indexOf("TR")==-1){
						words[23]="NA";
					}
					
					String track_7did=words[24].trim();
					if(track_7did.equals("")){
						words[24]="0";
					}else{
						Integer.parseInt(track_7did);
					}
					
					String year=words[25].trim();
					if(year.equals("")){
						words[25]="0";
					}else{
						Integer.parseInt(year);
					}
					
					String artist_mbid=words[26].trim();
					if(artist_mbid.equals("")){
						words[26]="NA";
					}
					
					String artist_mbtags=words[27].trim();
					if(artist_mbtags.indexOf("[")==-1){
						words[27]="[]";
					}
					
					String artist_mbcount=words[28].trim();
					if(artist_mbcount.indexOf("[")==-1){
						words[28]="[]";
					}
					
					String artist_playmeid=words[29].trim();
					if(artist_playmeid.equals("")||artist_playmeid.equals("-1")){
						words[29]="0";
					}else{
						Integer.parseInt(artist_playmeid);
					}
					
					
					String artist_terms=words[30].trim();
					if(artist_terms.indexOf("[")==-1){
						words[30]="[]";
					}
					
					String artist_termfrequency=words[31].trim();
					if(artist_termfrequency.indexOf("[")==-1){
						words[31]="[]";
					}
					
					String artist_termweight=words[32].trim();
					if(artist_termweight.indexOf("[")==-1){
						words[32]="[]";
					}
					
					String similar_artists=words[33].trim();
					if(similar_artists.indexOf("[")==-1){
						words[33]="[]";
					}
					
					

					StringBuilder valueBuilder=new StringBuilder();
					for(String s:words){
						valueBuilder.append(s).append("\t");
					}
					valueBuilder.append("0").append("\t").append("0");
					//valueBuilder.delete(valueBuilder.length()-2, valueBuilder.length()-1);
					//System.out.println(valueBuilder.toString());
					context.write(new Text(key.toString()),new Text(valueBuilder.toString().trim()) );
					

				}catch(Exception e){
					e.printStackTrace();
				}

				
			}else if(words.length>34){
				try{

					String analysis_sample_rate=words[0].trim();
					double asr=Double.parseDouble(analysis_sample_rate);

					if(asr!=22050){
						words[0]="22050";
					}


					String artist_7did=words[1].trim();
					if(artist_7did.equals("")){
						words[1]="NA";

					}

					String artist_familiarity=words[2].trim();
					Float.parseFloat(artist_familiarity);

					String artist_hotness=words[3].trim();
					Float.parseFloat(artist_hotness);

					String artist_id=words[4].trim();
					if(artist_id.indexOf("AR")==-1){
						words[4]="NA";
					}

					
					String artist_latitude=words[5].trim();
					
					
					
					if(artist_latitude.equals("")){
						words[5]="0";
					}else{
						Float.parseFloat(artist_latitude);
					}
					//System.out.println("artist_latitude"+artist_latitude);

					String artist_location=words[6].trim();
					if(artist_location.equals("")){
						words[6]="NA";
					}

					String artist_longitude=words[7].trim();
					
					if(artist_longitude.equals("")){
						words[7]="0";
					}else{
						Float.parseFloat(artist_longitude);
					}
					//System.out.println("artist_longitude"+artist_longitude);

					String artist_name=words[8].trim();
					if(artist_name.equals("")){
						words[8]="NA";
					}

					String audio_md5=words[9].trim();
					if(audio_md5.equals("")){
						words[9]="NA";
					}


					String number_of_beats=words[10].trim();
					if(number_of_beats.equals("")){
						words[10]="0";
					}else{
						Integer.parseInt(number_of_beats);
					}


					String danceability=words[11].trim();
					if(danceability.equals("")){
						words[11]="0";
					}else{
						Double.parseDouble(danceability);
					}

					String duration=words[12].trim();
					if(duration.equals("")){
						words[12]="0";
					}else{
						Float.parseFloat(duration);
					}

					String end_of_fade_in=words[13].trim();
					if(end_of_fade_in.equals("")){
						words[13]="0";
					}else{
						Float.parseFloat(end_of_fade_in);
					}

					String energy=words[14].trim();
					if(energy.equals("")){
						words[14]="0";
					}else{
						Double.parseDouble(energy);
					}


					String loudness=words[15].trim();
					if(loudness.equals("")){
						words[15]="0";
					}else{
						Float.parseFloat(loudness);
					}
					
					

					String release=words[16].trim();
					if(release.equals("")){
						words[16]="NA";
					}
					
					String release_7did=words[17].trim();
					if(release_7did.equals("")){
						words[17]="0";
					}else{
						Integer.parseInt(release_7did);
					}
					
					
					String hotness=words[18].trim();
					if(hotness.equals("")){
						words[18]="0";
					}else{
						Float.parseFloat(hotness);
					}
					
					
					String songid=words[19].trim();
					if(songid.indexOf("SO")==-1){
						words[19]="NA";
					}
					
					String start_of_fade_out=words[20].trim();
					if(start_of_fade_out.equals("")){
						words[20]="0";
					}else{
						Float.parseFloat(start_of_fade_out);
					}
					
					String tempo=words[21].trim();
					if(tempo.equals("")){
						words[21]="0";
					}else{
						Float.parseFloat(tempo);
					}
					
					String title=words[22].trim();
					if(title.equals("")){
						words[22]="NA";
					}
					
					String trackid=words[23].trim();
					if(trackid.indexOf("TR")==-1){
						words[23]="NA";
					}
					
					String track_7did=words[24].trim();
					if(track_7did.equals("")){
						words[24]="0";
					}else{
						Integer.parseInt(track_7did);
					}
					
					String year=words[25].trim();
					if(year.equals("")){
						words[25]="0";
					}else{
						Integer.parseInt(year);
					}
					
					String artist_mbid=words[26].trim();
					if(artist_mbid.equals("")){
						words[26]="NA";
					}
					
					String artist_mbtags=words[27].trim();
					if(artist_mbtags.indexOf("[")==-1){
						words[27]="[]";
					}
					
					String artist_mbcount=words[28].trim();
					if(artist_mbcount.indexOf("[")==-1){
						words[28]="[]";
					}
					
					String artist_playmeid=words[29].trim();
					if(artist_playmeid.equals("")||artist_playmeid.equals("-1")){
						words[29]="0";
					}else{
						Integer.parseInt(artist_playmeid);
					}
					
					
					String artist_terms=words[30].trim();
					if(artist_terms.indexOf("[")==-1){
						words[30]="[]";
					}
					
					String artist_termfrequency=words[31].trim();
					if(artist_termfrequency.indexOf("[")==-1){
						words[31]="[]";
					}
					
					String artist_termweight=words[32].trim();
					if(artist_termweight.indexOf("[")==-1){
						words[32]="[]";
					}
					
					String similar_artists=words[33].trim();
					if(similar_artists.indexOf("[")==-1){
						words[33]="[]";
					}
					
					
					String user_count=words[34].trim();
					if(user_count.equals("")){
						words[34]="0";
					}else{
						Integer.parseInt(user_count);
					}
					
					String num_played=words[35].trim();
					if(num_played.equals("")){
						words[35]="0";
					}else{
						Integer.parseInt(num_played);
					}

					StringBuilder valueBuilder=new StringBuilder();
					for(String s:words){
						valueBuilder.append(s).append("\t");
					}
					valueBuilder.delete(valueBuilder.length()-1, valueBuilder.length());
					
					//System.out.println(valueBuilder.toString());
					context.write(new Text(key.toString()),new Text(valueBuilder.toString().trim()) );
					

				}catch(Exception e){
					//e.printStackTrace();
				}
				
			}
			



			

		}

	}


	public static class SReducer extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			Iterator<Text> itr=values.iterator();
			if(itr.hasNext()){
				Text value=itr.next();
				context.write(null, value);
				
			}
			

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Data Sanitization");
		job.setJarByClass(DataSanitization.class);
		job.setMapperClass(SMapper.class);


		job.setReducerClass(SReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
