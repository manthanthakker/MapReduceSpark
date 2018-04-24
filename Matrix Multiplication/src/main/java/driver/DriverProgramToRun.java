package driver;

import inversemapper.InverseMapperDriver;
import mapper.PrepareMapperDriver;
import partitionbyrow.Driver;
import filterpagerankvector.DriverFilterPageRank;
import parserjob.ParserJob;
import sorter.SorterMapperDriver;

/**
 * @author Manthan Thakker
 * @project HW5
 * @date 3/26/18
 * @email thakker.m@husky.neu.edu
 */
public class DriverProgramToRun {

    /**
     * Page Rank Driver for Partioning by Row PageRank
     *
     * @param args
     */
    public static void main(String args[]) {
        try {

            String job1[] = {"input", "output1"};
            String job2[] = {"output1", "output2", "cache" + ""};
            String job3[] = {"output1", "output3", "cache"};
            String mapperinput[] = {"output1", "mapper"};
            String sorterInput[] = {"output3", "output4", "mapper"};
            String inverseMapper[] = {"output4", "output5", "mapper"};

            // Parses each file
            // and emits 4 types of nodes
            // 1. Node with unique Id key (#node)
            // 2. Node with  its inlinks and cj (#inlink)
            // 3. PageRank Node (#PR)
            ParserJob.main(job1);

            // Filter For PageRank (Collects all #PR nodes
            DriverFilterPageRank.main(job2);

            // Filter For getting all the nodes with unique Id and actual pageName
            // (#node ) (Index -> Name)
            PrepareMapperDriver.main(mapperinput);

            // PageRank Job for 10 Iterations
            Driver.main(job3);

            //Get the Top K algorithm
            SorterMapperDriver.main(sorterInput);

            //Get Names from the TopK indexes and output
           // InverseMapperDriver.main(inverseMapper);

        } catch (Exception exp) {
            System.out.println("Error in DriverProgramToRun " + exp);
        }
    }
}
