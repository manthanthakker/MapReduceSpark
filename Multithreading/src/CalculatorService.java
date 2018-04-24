
import java.util.HashMap;
import java.util.List;

/**
 * This class represents
 */
public class CalculatorService implements Calculator{

    /**
     *
     * @param stationTmax: Given HashMap which is populated with the StationId, list of Tmaxs
     * Computes the Average of all Tmaxs and prints on console.
     */
    public void printAverage(HashMap<String, List<Integer>> stationTmax){
        for (String stationId : stationTmax.keySet()) {
            List<Integer> tmaxs = stationTmax.get(stationId);
            double sum = 0.0;
            try {
                for (Integer tmax : tmaxs) {
                    sum += tmax;
                }
            } catch (Exception e) {

            }
            //System.out.println("Avg Tmax is: "+sum/tmaxs.size()+"for stationID: "+stationId);
        }
    }


    public long triggerRoutine() {
        return 0;
    }
}
