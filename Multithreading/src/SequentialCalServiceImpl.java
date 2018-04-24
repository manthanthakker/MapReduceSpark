

import loader.LoadFileService;
import loader.LoadFileServiceImpl;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;

import static loader.FibonacciCalculator.fib;


/**
 * This Class Represents the SequentialCalculator
 */
public class SequentialCalServiceImpl extends CalculatorService implements Calculator {

    // stationTmaxs: Key: StationId, Value: List of Tmax for respective stationId
    static HashMap<String, List<Integer>> records;

    //initializes the datastructures
    public SequentialCalServiceImpl() {
        records = new HashMap<String, List<Integer>>();
    }

    public static void main(String args[]) {

        LoadFileService loadFileService = new LoadFileServiceImpl();
        List<String> records = loadFileService.loadFile();
        SequentialCalServiceImpl sequentialCalService = new SequentialCalServiceImpl();
        sequentialCalService.computeAverage(records);

    }


    /**
     *
     * @param recordsList : each element is a single line from the input line with format
     *                    (StationId,Date,Type,Reading...)
     *  Pseudo code: Step1: Iterates through the list and populates the hashmap
     *               Step2: Calculates the average for each stationId
     *               Step3: Prints the Average TMAX
     */
    public void computeAverage(List<String> recordsList) {

        // Step1
        for (String record : recordsList) {
            String[] parameters = record.split(",");
            String stationId = parameters[0];
            String type = parameters[2];
            Integer reading = Integer.parseInt(parameters[3]);
            if (type.equals("TMAX")) {
                List<Integer> list = records.get(stationId);
                if (list == null)
                    list = new ArrayList<Integer>();
                list.add(reading);
                fib(17);
                records.put(stationId, list);
            }

        }

        //Step2
        for (String stationId : records.keySet()) {
            List<Integer> tmaxs = records.get(stationId);
            double sum = 0.0;
            for (Integer tmax : tmaxs) {
                sum += tmax;
            }
            //Step3: Commented for performance analysis
            // System.out.println("Station ID: "+stationId+"   TMAX AVERAGE: "+sum/tmaxs.size());
        }

    }
    /**
     * Triggers routine :
     * 1. Calls the loader routine to intialize the list of records containing each line as elements
     * 2. Computes the averageTmaxs for each station Id
     *
     * <p>
     * returns timeTaken: Time taken to copute the averages
     */
    public long triggerRoutine() {

        // Step1
        LoadFileService loadFileService = new LoadFileServiceImpl();
        List<String> records = loadFileService.loadFile();
        SequentialCalServiceImpl sequentialCalService = new SequentialCalServiceImpl();
        Long startTime = System.currentTimeMillis();

        // Step2
        sequentialCalService.computeAverage(records);

        Long endTime = System.currentTimeMillis();
        // System.out.println("Time required: " + (endTime - startTime));
        return (endTime - startTime);
    }


}
