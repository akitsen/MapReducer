import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, Text> {


	@Override
	protected void map(LongWritable offset, Text line, Context context) throws NumberFormatException, IOException, InterruptedException{
		String[] temp = line.toString().split(",");
		String urlRaw = temp[0];
		String url = urlRaw.substring(1, urlRaw.length()-1);
		String wordRaw = temp[1];
		String word = wordRaw.substring(1, wordRaw.length()-1);
		String num = temp[2];
		String numF = num.substring(0, num.length()-1);
		
		context.write(new Text(word), new Text(numF+","+url));
		
	}
}
