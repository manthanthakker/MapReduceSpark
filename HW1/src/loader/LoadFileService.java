package loader;

import java.io.File;
import java.util.List;

/**
 * This interface implements the loader routine for all the programs
 *
 */
public interface LoadFileService {

    /**
     *
     *
     * @return a list of Strings containing each record as a single element in the list.
     */
     List<String> loadFile();
}
