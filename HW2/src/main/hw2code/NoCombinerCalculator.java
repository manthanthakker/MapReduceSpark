package hw2code;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import hw2code.domain.TemperatureInfo;

import hw2code.domain.*;


/**
 * NoCombiner Calculator
 * -- Simple Map reduce program without any combiner / Partitioner
 */
public class NoCombinerCalculator {

    /**
     * No Combiner Calculator Driver Program. Run to trigger the job
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "No Combiner Run1");
        // Setup
        job.setJarByClass(NoCombinerCalculator.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setReducerClass(IntSumReducer.class);

        // Keys

        //Mapper
        job.setMapOutputValueClass(TemperatureInfo.class);
        job.setMapOutputKeyClass(Text.class);


        //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    /**
     * Mapper
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, TemperatureInfo> {

        /**
         * (line)=>(stationId,(typeOfTemp,Temperature))
         * @param key: Each line in the input file
         * @param value: Emits (typeOfTemp(0 for 1 for max and 2 for min),Temperature)
         * @param context : Hadoop context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreElements()) {
                Text stationId = new Text();
                String record[] = itr.nextToken().split(",");
                stationId.set(record[0]);

                if (record[2].equals("TMAX"))
                    context.write(stationId, new TemperatureInfo(1, Long.parseLong(record[3])));
                else if (record[2].equals("TMIN"))
                    context.write(stationId, new TemperatureInfo(2, Long.parseLong(record[3])));

            }
        }
    }


    /**
     * Reducer
     */
    public static class IntSumReducer
            extends Reducer<Text, TemperatureInfo, Text, Text> {


        /**
         * (stationId,list (typeOfTemp,Temperature))=>stationId,TemperatureMin,TemperatureMax
         * @param key : stationId
         * @param values: list of (typeOfTemp,Temperature)
         * @param context : Hadoop context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<TemperatureInfo> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sumMax = 0;
            double sumMin = 0;
            double countMax = 0;
            double countMin = 0;

            String output = key.toString();
            for (TemperatureInfo val : values) {
                if (val.getType() == 1) { // for TMAX
                    sumMax += val.getTemperature();
                    countMax++;
                } else { // Fot TMIN
                    sumMin += val.getTemperature();
                    countMin++;
                }
            }

            Text keyText = new Text();
            Text constantText = new Text();
            constantText.set(" ");
            keyText.set(output + "," + formatOutput(sumMax, sumMin, countMax, countMin));
            context.write(keyText, constantText);
        }
    }


    /**
     * Fomatter
     *
     * @param sumMax : Accumulated sum of TMAX
     * @param sumMin Accumulated sum of TMIN
     * @param countMax:  Accumulated num of TMAX
     * @param countMin: Accumulated num of TMIN
     * @return fomratter string to write in the document
     */
    public static String formatOutput(double sumMax, double sumMin, double countMax, double countMin) {
        StringBuilder sb = new StringBuilder();
        if (countMin != 0)
            sb.append(String.format("%.2f", sumMin / countMin) + ",");
        else
            sb.append(0 + ",");
        if (countMax != 0)
            sb.append(String.format("%.2f", sumMax / countMax));
        else
            sb.append(0);

        return sb.toString();
    }


}