import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class calculates the overall performance of each Routines by running each routing N number of times and
 * Displaying the AVERAGE, MIN and MAX runtime for each routine. It also outputs it in a file called report.txt
 *
 * HOW TO USE: 1. SET THE Parameter 'INPUTLOCATION' in LoadFileServiceImpl under loader package.
 *                Point to your text file.
 *             2. Set N=(Number of times you want each program to run) (By default 10)
 *             3. Simply run the main method from any ide.
 *             4. Outputs the time taken by each of 5 rountines on console and creates a file 'report.txt' with results.
 */
public class PerformanceAnalysis {


    static int N=10;
    public  static void main(String args[]) throws IOException {
        String output="";
        File report=new File("report.txt");
        FileWriter fw=new FileWriter(report);

        output+="SEQUENTIAL RUN: "+getStatsAfterRuns(new SequentialCalServiceImpl())+"\n";

        output+="NO SHARING RUN:"+getStatsAfterRuns(new NoSharingCalculator())+"\n";

        output+="COARSE LOCK RUN: "+getStatsAfterRuns(new CoarseLockCalculator())+"\n";

        output+="FINE LOCK RUN: "+getStatsAfterRuns(new FineLockCalculator())+"\n";

        output+="NO LOCK RUN :"+getStatsAfterRuns(new NoLockCalculator())+"\n";

        System.out.println(output);
        fw.write(output);
        fw.close();

    }

    /**
     *
     * @param calculatorService: Object of specific calculator
     * @return String calculates the average runtime after N runs of specific routines and outputs a text line of format
     *          (TMIN:X TMAX:Y TAVG: )"
     */
    public static String getStatsAfterRuns(CalculatorService calculatorService){
        List<Long> runTimes=new ArrayList<Long>();
        Double sum=0.0;
        for(int i=0;i<N;i++){
            Long runTime=calculatorService.triggerRoutine();
            runTimes.add(runTime);
            sum+=runTime;
        }
        return "TMIN : "+ Collections.min(runTimes)+" TMAX: "+Collections.max(runTimes)+" TAVG: "+sum/N;
    }
}
