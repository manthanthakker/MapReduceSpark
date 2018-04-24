
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    static Logger logger= Logger.getLogger("debugger");
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Writable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Logger log=Logger.getLogger("class");
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreElements()){
               // log.info("itr "+itr.nextToken());
                String record[]=itr.nextToken().split(",");

                word.set(record[0]);
                context.write(word,new Text(record[2]+" "+Long.parseLong(record[3])));
                //logger.info("debug" +record[2]+" "+Long.parseLong(record[3]));
            }
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Writable,Text,Writable> {
        private Writable result = new Text();

        public void reduce(Text key, Iterable<Writable> values,
                           Context context
        ) throws IOException, InterruptedException {


            long sumMax=0;
            long sumMin=0;
            long countMax=0;
            long countMin=0;

            for (Writable val : values) {
                String record[]=val.toString().split(" ");
                String type=record[0];
                String temp=record[1];
                if(type.equals("TMAX")){
                    sumMax+=Long.parseLong(temp);
                    countMax++;
                }
                if(type.equals("TMIN")){
                    sumMin+=Long.parseLong(temp);
                    countMin++;
                }
            }
            if(sumMax!=0&&sumMin!=0){
                result=(new Text(key.toString()+", "+(sumMax/countMax)+", "+(sumMin/countMin)));
                logger.info("debug reducer"+key.toString()+", "+(sumMax/countMax)+", "+(sumMin/countMin));
            }
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
//class Info implements Writable{
//    String stationId;
//    Long temp;
//    public Info(String stationId,Long temp){
//        this.stationId=stationId;
//        this.temp=temp;
//    }
//
//    public void write(DataOutput dataOutput) throws IOException {
//        dataOutput.writeBytes(stationId+" "+temp);
//    }
//
//    public void readFields(DataInput dataInput) throws IOException {
//       String input=dataInput.readLine();
//       String records[]=input.split(" ");
//       stationId=records[0];
//       temp=Long.parseLong(records[1]);
//    }
//}
