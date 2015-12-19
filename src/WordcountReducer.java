import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordcountReducer extends Reducer<Text, Text, Text, Text> {
	private Text word = new Text();
	private Text URLCount = new Text();
	@Override
	 protected void reduce(Text key, Iterable<Text> values, Context context)
		     throws IOException, InterruptedException {
		StringBuffer st = new StringBuffer();
		int count = 0;
		for(Text current : values){
		String[] temp  = current.toString().split(",");
		count += Integer.valueOf(temp[0]);
		st.append(temp[1].substring(1));
		st.append(" OCCURRENCES IN DOC = ");
		st.append(temp[0]+" ");
	}
		word.set(key);
		st.append(" TOTAL = ");
		st.append(count);
		URLCount.set(st.toString());
		context.write(word , new Text(URLCount));

		}
}
