package neu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class Reduce extends Reducer<Text,Text,Text,Text> {


	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> itr=values.iterator();
		StringBuilder sb=new StringBuilder();
		while(itr.hasNext()){
			sb.append(itr.next().toString()).append("\t");
		}

		context.write(null,new Text(sb.toString().trim()) );
	}
}