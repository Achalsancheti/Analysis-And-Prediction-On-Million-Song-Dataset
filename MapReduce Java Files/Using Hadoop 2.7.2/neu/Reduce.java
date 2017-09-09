package neu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class Reduce extends Reducer<Text,Text,Text,Text> {


	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> itr=values.iterator();
		StringBuilder sb=new StringBuilder();
		String one=null;
		String two=null;
		if(itr.hasNext()){
			 one=itr.next().toString();
		}
		if(itr.hasNext()){
			two=itr.next().toString();
		}
		
		//System.out.println("Reducer-----------------------------------------------");
		if(one!=null && two!=null){
			String bigger=one.length()>two.length()?one:two;
			String smaller=one.length()<two.length()?one:two;
			sb.append(bigger.trim()).append("\t").append(smaller.trim());
		}
		if(one==null && two.length()>20){
			sb.append(two.trim());
		}
		
		if(two==null && one.length()>20){
			sb.append(one.trim());
		}



		context.write(null,new Text(sb.toString().trim()) );
	}
}