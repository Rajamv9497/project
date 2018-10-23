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

public class question4 extends Configured implements Tool
{
   //Map class
	
   public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
   {
      public void map(LongWritable key, Text value, Context context)
      {
         try{
            String[] str = value.toString().split(",");
      
            String status=str[1];
            String cname=str[2];
            String year=str[7];
            String myrow = cname+","+year;
            context.write(new Text(cname), new Text(myrow));
         }
         catch(Exception e)
         {
            System.out.println(e.getMessage());
         }
      }
   }
   
   //Reducer class
	
   public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
   {
	   private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
      {        
    	  long count=0;
    	  String myvalue= "";
		  String mycount= "";
		  String my="";
       for(Text i:values)
       {
    	 count++; 
    	 my=i.toString();
       }
       mycount=String.format("%d",count);
       myvalue=my+","+mycount;
       repToRecordMap.put(new Long(count), new Text(myvalue));
       if (repToRecordMap.size() > 5) {
			repToRecordMap.remove(repToRecordMap.firstKey());
					}
       //context.write(key, new IntWritable(count));

      }
  	protected void cleanup(Context context) throws IOException,
	InterruptedException {
	for (Text t : repToRecordMap.values()) {
		// Output our five records to the file system with a null key
		context.write(NullWritable.get(), t);
		}
	}

   }
   
   //Partitioner class
	
   public static class CaderPartitioner extends
   Partitioner < Text, Text >
   {
      @Override
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split(",");
         if((Integer.parseInt(str[1]))==2011)
         {
            return 0;
         }
         else if((Integer.parseInt(str[1]))==2012)
         {
            return 1;
         }
         else if((Integer.parseInt(str[1]))==2013)
         {
            return 2;
         }
         else if((Integer.parseInt(str[1]))==2014)
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
	  job.setJarByClass(question4.class);
	  job.setJobName("Year Wise top 5 employer details");
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
      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(Text.class);
		
      System.exit(job.waitForCompletion(true)? 0 : 1);
      return 0;
   }
   
   public static void main(String ar[]) throws Exception
   {
      ToolRunner.run(new Configuration(), new question4(),ar);
      System.exit(0);
   }
}







