package hw2code;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
import hw2code.domain.TemperatureStats;

public class CombinerCalculator {

    /**
     * No Combiner Calculator Driver Program. Run to trigger the job
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, TemperatureStats> {


        private Text stationId = new Text();

        /**
         * (line)=>(stationId,TemperatureStats(sumMax,sumMin,countMax,countMin))
         *
         * @param key:    Each line in the input file
         * @param value:  Emits (stationId,TemperatureStats(sumMax,sumMin,countMax,countMin))
         * @param context : Hadoop context writes the list of values
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreElements()) {

                String record[] = itr.nextToken().split(",");

                stationId.set(record[0]);

                if (record[2].equals("TMAX"))
                    context.write(stationId, new TemperatureStats(Long.parseLong(record[3]), 0l, 1l, 0l));
                else if (record[2].equals("TMIN"))
                    context.write(stationId, new TemperatureStats(0l, Long.parseLong(record[3]), 0l, 1l));

            }
        }
    }

    /**
     * Combiner
     */
    public static class IntCombiner extends Reducer<Text, TemperatureStats, Text, TemperatureStats> {

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
                           Context context) throws InterruptedException, IOException {
            double sumMax = 0l;
            double sumMin = 0l;
            double countMax = 0l;
            double countMin = 0l;
            for (TemperatureStats temperatureStats : values) {
                sumMax += temperatureStats.getSumMax();
                sumMin += temperatureStats.getSumMin();
                countMax += temperatureStats.getCountMax();
                countMin += temperatureStats.getCountMin();
            }
            context.write(key, new TemperatureStats(sumMax, sumMin, countMax, countMin));
        }

    }

    /**
     * Reducer
     */
    public static class IntSumReducer
            extends Reducer<Text, TemperatureStats, Text, Text> {


        /**
         * (stationId,TemperatureStats(sumMax,sumMin,countMax,countMin))=>stationId,TemperatureMin,TemperatureMax
         *
         * @param key     : stationId
         * @param values: list of TemperatureStats(sumMax,sumMin,countMax,countMin)
         * @param context : Hadoop context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<TemperatureStats> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sumMax = 0;
            int sumMin = 0;
            int countMax = 0;
            int countMin = 0;


            for (TemperatureStats val : values) {
                sumMax += val.getSumMax();
                countMax += val.getCountMax();
                sumMin += val.getSumMin();
                countMin += val.getCountMin();
            }
            StringBuilder output = new StringBuilder();
            output.append(key.toString() + ",");
            Text keyText = new Text();
            Text constantText = new Text();
            constantText.set(" ");
            keyText.set(output + formatOutput(sumMax, sumMin, countMax, countMin));
            context.write(keyText, constantText);

        }
    }

    /**
     * Fomatter
     *
     * @param sumMax : Accumulated sum of TMAX
     * @param sumMin  :Accumulated sum of TMIN
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
     * Driver Program
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "No Combiner");
        // Setup
        job.setJarByClass(CombinerCalculator.class);
        job.setMapperClass(CombinerCalculator.TokenizerMapper.class);

        job.setReducerClass(CombinerCalculator.IntSumReducer.class);
        job.setCombinerClass(IntCombiner.class);


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
