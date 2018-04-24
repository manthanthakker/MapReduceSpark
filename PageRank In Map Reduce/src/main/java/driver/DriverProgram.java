package driver;

import pagerank.PageRankImpl;
import parserjob.ParserJob;
import topk.TopK;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Manthan Thakker
 * @project HW3
 * @date 2/22/18
 * @email thakker.m@husky.neu.edu
 */
public class DriverProgram {

    /**
     * Initiates the execution
     * @param args: The input and the ouput paths
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {

        // Phase 1
        final String dataSetInput;
        final String dataSetOutput;

        // Phase 2
        final String pageRankInput;
        final String pageRankOutput;

        // Phase 3
        long UNIQUEPAGES;
        final String topKInput;
        final String topKoutput;

        long K=10;


        topKInput = args[1] + "/10";
        topKoutput = args[1] + "/output";

        String commandLine[] = new String[4];
        commandLine[0] = args[0];
        commandLine[1] = args[1];
        UNIQUEPAGES = ParserJob.main(commandLine);

        commandLine[0] = args[1];
        commandLine[2] = UNIQUEPAGES + "";
        PageRankImpl.main(commandLine);

        commandLine[0] = topKInput;
        commandLine[1] = topKoutput;
        commandLine[2] = UNIQUEPAGES + "";
        commandLine[3]=K+"";
        TopK.main(commandLine);

    }


}
