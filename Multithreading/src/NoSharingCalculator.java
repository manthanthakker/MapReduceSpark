import loader.LoadFileService;
import loader.LoadFileServiceImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static loader.FibonacciCalculator.fib;


/**
 * This Class Represents the NoSharingCalculator
 */
public class NoSharingCalculator extends  CalculatorService implements Runnable{

    // records : represents each line from the data set(1912.csv)
    List<String> records;

    // stationTmaxs: Key: StationId, Value: List of Tmax for respective stationId accessed by Thread Odd
    static HashMap<String,List<Integer>> oddStationTmax;
    // stationTmaxs: Key: StationId, Value: List of Tmax for respective stationId accessed by Thread Even
    static HashMap<String,List<Integer>> evenStationTmax;
    // stationTmaxs: Key: StationId, Value: List of Tmax for respective stationId accessed by main Thread
    static HashMap<String,List<Integer>> allStationTmax;

    //initializes the datastructures
    public NoSharingCalculator(List<String> records) {
        oddStationTmax = new HashMap<String, List<Integer>>();
        evenStationTmax = new HashMap<String, List<Integer>>();
        allStationTmax=new HashMap<String, List<Integer>>();
        this.records = records;
    }
    public NoSharingCalculator(){
        oddStationTmax = new HashMap<String, List<Integer>>();
        evenStationTmax = new HashMap<String, List<Integer>>();
        allStationTmax=new HashMap<String, List<Integer>>();
    }

    public static void main(String args[]) {

        LoadFileService loadFileService = new LoadFileServiceImpl();
        List<String> allrecords = loadFileService.loadFile();

        CoarseLockCalculator calculator = new CoarseLockCalculator(allrecords);

        Thread t1 = new Thread(calculator, "odd");
        Thread t2 = new Thread(calculator, "even");
        Long startTime = System.currentTimeMillis();
        t1.start();
        t2.start();

        while (t1.isAlive() || t2.isAlive()) {

        }
        calculator.printAverage(allStationTmax);
        Long endTime = System.currentTimeMillis();
        System.out.println("Time required: " + (endTime - startTime));

    }


    /**
     * Triggers routine :
     * 1. Calls the loader routine to intialize the list of records containing each line as elements
     * 2. Creates two threads(Sync on hashmap) :
     *      a. Odd : Processes the odd elements in the list, stores records in oddStationTmax
     *      b. Even: Processes the even elements in the list, store the records in evenStationTmax
     * 3. Combines both hashmaps oddStationTmax && evenStationTmax -> allStationTmax
     * 4. Computes average using the hashmap allStationTmax
     * <p>
     * returns timeTaken: Time taken to copute the averages
     */
    public long triggerRoutine(){

        //Step1
        LoadFileService loadFileService = new LoadFileServiceImpl();
        List<String> allrecords = loadFileService.loadFile();

        //Step2
        NoSharingCalculator calculator = new NoSharingCalculator(allrecords);
        Thread t1 = new Thread(calculator, "odd");
        Thread t2 = new Thread(calculator, "even");
        Long startTime = System.currentTimeMillis();
        t1.start();
        t2.start();

        try{
            t1.join();
            t2.join();
        }catch (Exception e){

        }

        //Step 3
        combineOddAndEven();

        //Step 4
        calculator.printAverage(allStationTmax);
        Long endTime = System.currentTimeMillis();
        //   System.out.println("Time required: " + (endTime - startTime));
        return (endTime - startTime);
    }


    /**
     * Given oddStationTmax,evenStationTmax populated
     * Populates allStationTmax by combining  oddStationTmax and evenStationTmax.
     *
     */
    public void combineOddAndEven(){
        for(String stationId:oddStationTmax.keySet()){
            List<Integer> tmaxs=oddStationTmax.get(stationId);
            List<Integer> evenTmax=evenStationTmax.get(stationId);
            if(evenTmax!=null)
            tmaxs.addAll(evenTmax);
            evenStationTmax.remove(stationId);

            allStationTmax.put(stationId,tmaxs);
        }
        if(evenStationTmax!=null)
        allStationTmax.putAll(evenStationTmax);
    }


    /**
     * Pseudo Code:
     * Step1: Checks If the thread is odd or even and initialized index respectively.
     * Step2: Iterates over the list and Odd thread processes odd elements and evenThread even elements and inserts Tmax
     * into the hashmap if not present or appends to the list of respective stationID. No synchornization
     */
    public void run() {
        int index = 0;
        String threadName=Thread.currentThread().getName();
        if (threadName.equals("odd"))
            index = 1;
        while (index < records.size()) {

            String record = records.get(index);
            String[] parameters = record.split(",");
            String stationId = parameters[0];
            String type = parameters[2];
            Integer reading = Integer.parseInt(parameters[3]);


            insertNewTmax(type,stationId,reading,threadName);


            index += 2;
        }
    }

    /**
     *
     * @param type: The type of thread : TMAX, TMIN...
     * @param stationId: The stationId for which the new Tmax is to be inserted
     * @param reading: The Tmax value
     * @param threadName: Odd or Even
     *                  Inserts a TMax into the odd or even hashmap based on the threadType.
     */
    public static void insertNewTmax(String type,String stationId,Integer reading,String threadName){
        if (type.equals("TMAX")) {
            List<Integer> list= null;

             list=threadName.equals("odd")?oddStationTmax.get(stationId):evenStationTmax.get(stationId);

            if (list == null)
                list = new ArrayList<Integer>();
            list.add(reading);
            // Added for delay
            fib(17);
            if(threadName.equals("odd"))
                oddStationTmax.put(stationId, list);
            else
                evenStationTmax.put(stationId,list);
        }
    }
}
