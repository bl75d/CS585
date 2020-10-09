//https://www.edureka.co/blog/mapreduce-example-reduce-side-join/
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Spatial_join {
   public static class pointmapper extends Mapper<Object, Text, Text, Text>{
	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		   int window_size=100;//define a 100*100 window
		   String data=value.toString();
		   String[] pnt=data.split("(|,|)");
		   int x=Integer.parseInt(pnt[0]);
		   int y=Integer.parseInt(pnt[1]);
		   String index=Integer.toString((y/window_size)*10+(x/window_size));
		   context.write(new Text(index), new Text(value));
	   }
   }
   public static class rectanglemapper extends Mapper<Object, Text, Text, Text>{
		protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			   int window_size=100;//define a 100*100 window
			   String data=value.toString();
			   String[] rec=data.split("<|,|>");
			   int h=Integer.parseInt(rec[2]);
               int w=Integer.parseInt(rec[3]);
			   int top_leftx=Integer.parseInt(rec[0]);
			   int top_lefty=Integer.parseInt(rec[1])-h;
			   
//			   ArrayList<String> index = new ArrayList<String>();
			   int start=(top_leftx/window_size)*10+(top_lefty/window_size);
			   for(int i=0;i<w/window_size;i++) {
				   for(int j=0;j<h/window_size;j++) {
//					   index.add(Integer.toString(start+i+10*j));
					   String windowindex=Integer.toString(start+i+10*j);
					   context.write(new Text(windowindex), new Text(value));
				   }
			   }
		   }
	   }
   			
   	public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
   		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   			
   		}
   	}
   
   
}
