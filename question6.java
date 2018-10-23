import java.io.*;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;
import java.text.DecimalFormat;
public class question6 extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split(",");
      
            String status=str[1];
            String year=str[7];
            context.write(new Text(status), new Text(year));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,Text,Text>
   {
    

      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
      {
    	  int count1=1551;
    	  int count=0;
    	  String years ="";
    	 for(Text i:values)
    	 {
    		 count++;
    		 years = i.toString();
    	 }
    	 DecimalFormat df = new DecimalFormat("###.##");
    	 double res1 = (double)count*100.0/(double)count1;
    	 String res = df.format(res1);
    	 String op = String.format("%d,%s",count,res);
       context.write(key,new Text(years+","+op+"%"));

      }
      
  	

   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
        String a=value.toString(); 
         if(a.equals("2011"))
         {
            return 0;
         }
         else if(a.equals("2012"))
         {
            return 1;
         }
         else if(a.equals("2013"))
         {
            return 2;
         }
         else if(a.equals("2014"))
         {
            return 3;
         }
         else
         {
        	return 4;
         }
        	 
         
      }
      
   }
   

   public int run(String[] arg) throws Exception
   {
	
	   
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf);
	  job.setJarByClass(question6.class);
	  job.setJobName("Case status percentage yearwise");
      FileInputFormat.setInputPaths(job, new Path(arg[0]));
      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
		
      job.setMapperClass(MapClass.class);
		
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      
      //set partitioner statement
		
      job.setPartitionerClass(CaderPartitioner.class);
      job.setReducerClass(ReduceClass.class);
      job.setNumReduceTasks(5);
      job.setInputFormatClass(TextInputFormat.class);
		
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
   }
   
   public static void main(String ar[]) throws Exception
   {
      ToolRunner.run(new Configuration(), new question6(),ar);
      System.exit(0);
   }
}







