package hw2code.comparators;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import hw2code.domain.StationIdTime;

import hw2code.domain.*;

public class CustomGroupingComparator extends WritableComparator {
    /**
     * Constructor Calls
     */
    CustomGroupingComparator(){
        super(StationIdTime.class,true);
    }


    /**
     * Overrriding the compare method
     * @param wc1
     * @param wc2
     * @return
     */
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        StationIdTime record1 = (StationIdTime) wc1;
        StationIdTime record2 = (StationIdTime) wc2;
        return record1.getStationId().compareTo(record2.getStationId());

    }


}
