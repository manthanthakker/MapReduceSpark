package hw2code;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import hw2code.domain.TemperatureStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import hw2code.*;

public class InMapperCombCalculator {


    /**
     * Mapper
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, TemperatureStats> {

        private Text stationId = new Text();
        private HashMap<String, TemperatureStats> stationRecord;

        /**
         * Initializes the hashmap before every map task
         * @param context
         */
        public void setup(Context context) {
            stationRecord = new HashMap<>();
        }


        /**
         * (line)=>(stationId,(typeOfTemp,Temperature))
         * @param key: Each line in the input file
         * @param value: Inserts a record in hashmap with key as station Id and value as TemperatureStats
         * @param context : Hadoop context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreElements()) {

                String record[] = itr.nextToken().split(",");
                stationId.set(record[0]);
                TemperatureStats temperatureStats = stationRecord.get(record[0]);
                if (temperatureStats == null) {
                    stationRecord.put(record[0], new TemperatureStats(0l, 0l, 0l, 0l));
                    temperatureStats = stationRecord.get(record[0]);
                }

                if (record[2].equals("TMAX")) {
                    temperatureStats.setCountMax(temperatureStats.getCountMax() + 1);
                    temperatureStats.setSumMax(temperatureStats.getSumMax() + Long.parseLong(record[3]));
                    stationRecord.put(record[0], temperatureStats);
                } else if (record[2].equals("TMIN")) {
                    temperatureStats.setCountMin(temperatureStats.getCountMin() + 1);
                    temperatureStats.setSumMin(temperatureStats.getSumMin() + Long.parseLong(record[3]));
                    stationRecord.put(record[0], temperatureStats);
                }

            }
        }

        /**
         * On finishing all input iterates over the hashmap and emits
         * Accumulated TemperatureStats (stationId,TemperatureStats(sumMax,sumMin,countMax,countMin))
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text keyText = new Text();
            for (String stationId : stationRecord.keySet()) {
                keyText.set(stationId);
                context.write(keyText, stationRecord.get(stationId));
            }
        }
    }


    /**
     * Reducer
     */
    public static class IntSumReducer
            extends Reducer<Text, TemperatureStats, Text, Text> {

        /**
         *
         * Simply accumulates all the records for a stationId, combines them adding all the accumulators
         * and sends for reducing
         * list (stationId,TemperatureStats(sumMax,sumMin,countMax,countMin))=>
         *        (stationId,TemperatureStats(sumMax,sumMin,countMax,countMin))
         * @param key: StationId
         * @param values :  TemperatureStats(sumMax, sumMin, countMax, countMin))
         * @param context
         * @throws InterruptedException
         * @throws IOException
         */
        public void reduce(Text key, Iterable<TemperatureStats> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sumMax = 0;
            double sumMin = 0;
            double countMax = 0;
            double countMin = 0;
            StringBuilder output = new StringBuilder();
            output.append(key.toString());
            for (TemperatureStats val : values) {
                sumMax += val.getSumMax();
                countMax += val.getCountMax();
                sumMin += val.getSumMin();
                countMin += val.getCountMin();
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
     * @param sumMax    : Accumulated sum of TMAX
     * @param sumMin    Accumulated sum of TMIN
     * @param countMax: Accumulated num of TMAX
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


    /**
     *
     * Driver Program
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InMapperCombiner");
        // Setup
        job.setJarByClass(InMapperCombCalculator.class);
        job.setMapperClass(InMapperCombCalculator.TokenizerMapper.class);
        job.setReducerClass(InMapperCombCalculator.IntSumReducer.class);


        // Keys
        //Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TemperatureStats.class);


        //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
