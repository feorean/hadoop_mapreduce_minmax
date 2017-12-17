package com.fx.minmaxfx;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

  public static class ReaderMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private Text YearCountry = new Text();
    private final static String comma = ",";
    
    private DoubleWritable rate = new DoubleWritable();
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    
    try {	
      //Split columns
      String[] columns = value.toString().split(comma);
      
      //Set FX rate
      rate.set(Double.parseDouble(columns[2]));
      
      //Construct key
      YearCountry.set(columns[0].substring(0, 3) +"_"+ columns[1]);
      
      //Submit value into the Context
      context.write(YearCountry, rate);
    }
    catch (NumberFormatException ex) {
    	System.out.println(value.toString());
    	throw ex;
    }
    }
  }

  public static class MinMaxReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable minW = new DoubleWritable();
    private DoubleWritable maxW = new DoubleWritable();
    
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double min = 0;
      double max = 0;
      
      for (DoubleWritable val : values) {
        min = val.get()<min?val.get():min;
        max = val.get()>max?val.get():max;
      }
      minW.set(min);
      maxW.set(max);
      
      //Set key as Year Country Min/Max
      
      Text minKey = new Text(key.toString()+"_"+"MIN");
      Text maxKey = new Text(key.toString()+"_"+"MAX");
      
      context.write(minKey, minW);
      context.write(maxKey, maxW);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Min Max FX by YEAR");
    job.setJarByClass(App.class);
    job.setMapperClass(ReaderMapper.class);
    //job.setCombinerClass(MinMaxReducer.class);
    job.setReducerClass(MinMaxReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
