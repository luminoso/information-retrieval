package pt.ua.deti.ir;

/**
 * Configuration constants
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Constants {
	
    public static final int MINIMUM_WORD_LENGTH = 2;

    // group 1: Id
    // group 2: CreationDate
    // group 3: Score
    // group 4: FilePath
    // group 5: Body
    public static final String CORPUS_REGEX_DOCUMENT = "Id:([\\d]*?),CreationDate:(.*?),Score:([\\d]*?),FilePath:(.*?),Body:(.*)";

    public static final String ASCII_WORD_REGEX_MATCH = "([a-zA-Z0-9]+)";

    public static final String CORPUS_FILE_EXTENSION = ".csv";

    public static final int CORPUS_COUNT_HINT = 3165237; // not zero

    public static final String STATS_FILE = "processing.stats";
}
