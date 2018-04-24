package loader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class LoadFileServiceImpl implements LoadFileService {


    String INPUTLOCATION="/Users/trailbrazer/Desktop/MR/input files/1912.csv";
    /**
     *
     *
     * @return a list of Strings containing each record as a single element in the list.
     */
    public List<String> loadFile() {
        File file = new File(INPUTLOCATION);
        List<String> records=new ArrayList();
        try {
            BufferedReader bf = new BufferedReader(new FileReader(file));
            String line="";
            while((line=bf.readLine())!=null) {
                records.add(line);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException io){
            io.printStackTrace();
        }

        return records;
    }
}
