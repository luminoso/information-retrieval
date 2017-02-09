package pt.ua.deti.ir.Structures;

import pt.ua.deti.ir.Constants;
import pt.ua.deti.ir.Log;
import pt.ua.deti.ir.Utils.Disk.DiskManager;

import java.io.File;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Document class. A document has an ID and the respective content. The content starts ans a single string, that is
 * splitted, tokenized, stemmed, etc..
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class CorpusStatistics implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(Log.class.getName());
    private final String dirname;
    private BigInteger tokenCount;
    private BigInteger corpusCount;

    /**
     * Initializes a CorpusStatistics
     * @param dirname the directory name of where to store CorpusStatistics on disk
     */
    public CorpusStatistics(String dirname) {
        tokenCount = new BigInteger("0");
        corpusCount = new BigInteger("0");
        this.dirname = new File(dirname).getPath();
    }

    /**
     * Synchronized increment of the token count
     * @param value
     */
    public synchronized void incTokenCount(int value) {
        String previous = tokenCount.toString();

        BigInteger bi = new BigInteger(String.valueOf(value));
        tokenCount = tokenCount.add(bi);

        LOGGER.log(Level.FINEST, "Incremented token count from {0} to {1}", new Object[]
                {
                        previous, tokenCount.toString()
                });
    }

    /**
     * Save CorpusStatistics data to disk
     */
    public void save() {
        DiskManager dm = new DiskManager(dirname);
        dm.write(Constants.STATS_FILE, this);
    }

    /**
     * Getter - corpus count
     * @return
     */
    public int getCorpusCount() {
        return corpusCount.intValueExact();
    }

    /**
     * Synchronized setter -  corpus count
     * @param value
     */
    public synchronized void setCorpusCount(int value) {
        String previous = corpusCount.toString();

        corpusCount = new BigInteger(String.valueOf(value));

        LOGGER.log(Level.FINEST, "Incremented corpus count from {0} to {1}", new Object[]
                {
                        previous, corpusCount.toString()
                });
    }
    
    /**
     * Getter - token count
     * @return
     */
    public double getTokenCount() {
        return tokenCount.intValueExact();
    }

}
