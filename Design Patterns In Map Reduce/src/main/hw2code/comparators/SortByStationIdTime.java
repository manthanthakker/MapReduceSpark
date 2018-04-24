package hw2code.comparators;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import hw2code.domain.StationIdTime;

import hw2code.domain.*;

public class SortByStationIdTime extends WritableComparator {


    public SortByStationIdTime(){
        super(StationIdTime.class,true);
    }


    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {

       StationIdTime stationIdTime1=(StationIdTime)o1;
       StationIdTime stationIdTime2=(StationIdTime)o2;

       if(stationIdTime1.getStationId().compareTo(stationIdTime2.getStationId())==0){
           return stationIdTime1.getTime().compareTo(stationIdTime2.getTime());
       }
       else{
           return stationIdTime1.getStationId().compareTo(stationIdTime2.getStationId());
       }
    }



}
