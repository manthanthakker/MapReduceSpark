package hw2code;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import hw2code.comparators.CustomGroupingComparator;
import hw2code.comparators.SortByStationIdTime;
import hw2code.domain.SecondarySortRecordCombiner;
import hw2code.domain.StationIdTime;


import java.io.IOException;
import java.util.StringTokenizer;

public class SecondarySort {

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
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(SecondarySort.TokenizerMapper.class);

        job.setReducerClass(SecondarySort.IntSumReducer.class);
        job.setSortComparatorClass(SortByStationIdTime.class);
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        job.setPartitionerClass(CustomPartitioner.class);

        // Keys

        //Mapper
        job.setMapOutputKeyClass(StationIdTime.class);
        job.setMapOutputValueClass(SecondarySortRecordCombiner.class);


        //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Mappper
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, StationIdTime, SecondarySortRecordCombiner> {


        private Text stationIdText = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreElements()) {

                String record[] = itr.nextToken().split(",");
                String stationId = record[0];
                stationIdText.set(stationId);
                String time = record[1];

                if (record[2].equals("TMAX"))
                    context.write(new StationIdTime(stationId, time), new SecondarySortRecordCombiner(time, Long.parseLong(record[3]), 0l, 1l, 0l));
                else if (record[2].equals("TMIN"))
                    context.write(new StationIdTime(stationId, time), new SecondarySortRecordCombiner(time, 0l, Long.parseLong(record[3]), 0l, 1l));

            }
        }
    }

    /**
     * Custom Partitioner
     */
    public static class CustomPartitioner extends Partitioner<StationIdTime, SecondarySortRecordCombiner> {

        @Override
        public int getPartition(StationIdTime o, SecondarySortRecordCombiner o2, int i) {
            StationIdTime stationIdTime = o;
            return Math.abs(stationIdTime.getStationId().hashCode()) % i;
        }
    }


    /**
     * Reduceer
     */
    public static class IntSumReducer
            extends Reducer<StationIdTime, SecondarySortRecordCombiner, Text, Text> {


        public void reduce(StationIdTime key, Iterable<SecondarySortRecordCombiner> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sumMax = 0;
            double sumMin = 0;
            double countMax = 0;
            double countMin = 0;
            StringBuilder output = new StringBuilder();
            output.append(key.getStationId() + ", [");
            String currentYear = key.getTime().substring(0, 4);
            String prevYear = key.getTime().substring(0, 4);
            for (SecondarySortRecordCombiner val : values) {
                currentYear = val.getYear().substring(0, 4);

                if (currentYear.equals(prevYear)) {
                    sumMax += val.getSumMax();
                    countMax += val.getCountMax();
                    sumMin += val.getSumMin();
                    countMin += val.getCountMin();
                } else {
                    output.append(formatOutput(prevYear, sumMax, sumMin, countMax, countMin) + ",");
                    sumMax = val.getSumMax();
                    countMax = val.getCountMax();
                    sumMin = val.getSumMin();
                    countMin = val.getCountMin();
                    prevYear = currentYear;
                }
            }
            output.append(formatOutput(currentYear, sumMax, sumMin, countMax, countMin));


            Text keyText = new Text();
            Text constantText = new Text();
            constantText.set(" ");
            keyText.set(output.toString() + "]");
            context.write(keyText, constantText);
            //logger.info("debug reducer " +key.toString()+", "+(sumMax/countMax)+", "+(sumMin/countMin));

        }
    }

    /**
     * Fomatter
     *
     * @param currentyear
     * @param sumMax
     * @param sumMin
     * @param countMax
     * @param countMin
     * @return
     */
    public static String formatOutput(String currentyear, double sumMax, double sumMin, double countMax, double countMin) {
        StringBuilder sb = new StringBuilder();
        sb.append("(" + currentyear + ",");

        if (countMin != 0.0)
            sb.append(sumMin / countMin + ",");
        else
            sb.append(0 + ",");

        if (countMax != 0.0)
            sb.append(sumMax / countMax);
        else
            sb.append(0);
        sb.append(")");
        return sb.toString();

    }


}
