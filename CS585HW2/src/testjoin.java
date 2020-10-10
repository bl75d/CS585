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


public class testjoin {

   			

	 public static void main(String[] args) throws Exception {
     String a="(142,123)";
     System.out.println(a);
	 String[] pnt=a.split("\\(|,|\\)");
//     String[] b=a.toString().split("\\>|,|\\<| ");
//     System.out.println(pnt[0]);
     System.out.println(pnt.length);

//     System.out.println(pnt[2]);
//     System.out.println(b[3]);

   
	 } 
}
