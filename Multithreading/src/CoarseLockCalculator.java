import loader.LoadFileService;
import loader.LoadFileServiceImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static loader.FibonacciCalculator.fib;


/**
 * This Class Represents the CoarseLock Calculator
 */
public class CoarseLockCalculator extends CalculatorService implements Runnable {


    // records : represents each line from the data set(1912.csv)
    List<String> records;

    // stationTmaxs: Key: StationId, Value: List of Tmax for respective stationId
    static HashMap<String, List<Integer>> stationTmaxs;

    //initializes the datastructures
    public CoarseLockCalculator(List<String> records) {
        stationTmaxs = new HashMap<String, List<Integer>>();
        this.records = records;
    }

    public CoarseLockCalculator(){
        stationTmaxs = new HashMap<String, List<Integer>>();
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
        calculator.printAverage(stationTmaxs);
        Long endTime = System.currentTimeMillis();
        System.out.println("Time required: " + (endTime - startTime));

    }

    /**
     * Triggers routine :
     * 1. Calls the loader routine to intialize the list of records containing each line as elements
     * 2. Creates two threads(Sync on hashmap) : a. Odd : Processes the odd elements in the list
     * b. Even: Processes the even elements in the list
     * 3. After both threads are completed, computes average using the hashmap
     * <p>
     * returns timeTaken: Time taken to copute the averages
     */
    public long triggerRoutine() {
        // Step 1
        LoadFileService loadFileService = new LoadFileServiceImpl();
        List<String> allrecords = loadFileService.loadFile();

        //Step 2
        CoarseLockCalculator calculator = new CoarseLockCalculator(allrecords);
        Thread t1 = new Thread(calculator, "odd");
        Thread t2 = new Thread(calculator, "even");
        Long startTime = System.currentTimeMillis();
        t1.start();
        t2.start();

        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Step 3
        calculator.printAverage(stationTmaxs);
        Long endTime = System.currentTimeMillis();
        // System.out.println("Time required: " + (endTime - startTime));
        return (endTime - startTime);
    }

    /**
     * Pseudo Code:
     * Step1: Checks If the thread is odd or even and initalized index respectively.
     * Step2: Iterates over the list and Odd thread processes odd elements and evenThread even elements and inserts Tmax
     * into the hashmap if not present or appends to the list of respective stationID. synchronized over the
     */
    public void run() {
        //Step1
        int index = 0;
        if (Thread.currentThread().getName().equals("odd"))
            index = 1;

        //Step2
        while (index < records.size()) {

            String record = records.get(index);
            String[] parameters = record.split(",");
            String stationId = parameters[0];
            String type = parameters[2];
            Integer reading = Integer.parseInt(parameters[3]);

            if (type.equals("TMAX")) {

                //Synchronized on the entire data structure -- stationTmaxs
                synchronized (stationTmaxs) {
                    List<Integer> list = stationTmaxs.get(stationId);
                    if (list == null)
                        list = new ArrayList<Integer>();
                    list.add(reading);
                    fib(17);
                    stationTmaxs.put(stationId, list);
                }

            }

            index += 2;
        }
    }
}


