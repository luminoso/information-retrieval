package pt.ua.deti.ir.Reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import pt.ua.deti.ir.Log;
import pt.ua.deti.ir.Utils.Mem.MemMgr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The purpose of the CSVParser is to parse csv files.
 * It implements Runnable so it can be instantiated as Thread object.
 * It also contains garbage collector calls to force a sligthly better house keeping.
 * 
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class CSVParser implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(Log.class.getName());
    private final Queue<String> fileQueue;
    private final MemMgr mgr;
    private final Lock lock = new ReentrantLock();
    private final Condition parseMore = lock.newCondition();
    private final Condition parsingdone = lock.newCondition();
    private final Condition firstRunWait = lock.newCondition();
    private Iterator<CSVRecord> iterator;
    private String currentFilepath;
    private ConcurrentLinkedQueue<String> csvStrings;
    private boolean everythingParsed = false;
    private boolean everythingPooled = false;
    private boolean firstParse;
    private int targetNrDocuments;
    private int i;

    /**
     * Initializes a CSVParser
     * @param queue A CSV filepaths queue
     * @param mgr an instance of the MemMgr
     */
    public CSVParser(ConcurrentLinkedQueue<String> queue, MemMgr mgr) {
        this.firstParse = true;
        fileQueue = queue;
        this.mgr = mgr;
        initializeReaders();
        csvStrings = new ConcurrentLinkedQueue<>();
        targetNrDocuments = 50000;
    }

    @Override
    public void run() {

        StringJoiner sj;

        while (!everythingParsed)
            try {
                lock.lock();

                LOGGER.finest("Reading from file...");

                for (i = 0; ((BooleanSupplier) () ->
                {
                    // 0.3 overflows with -Xmx4g
                    return firstParse ? mgr.getUsedMem() < mgr.getMaxMem() * 0.20 : i < targetNrDocuments && !everythingParsed;

                }).getAsBoolean(); i++) {
                    if (!iterator.hasNext()) {
                        // no more records to process in this file. is there more?
                        if (!fileQueue.isEmpty()) {
                            initializeReaders();
                        } else {
                            everythingParsed = true;
                            break;
                        }
                    }

                    CSVRecord record = iterator.next();

                    sj = new StringJoiner(",");
                    try {

                        //Id,OwnerUserId,CreationDate,ClosedDate,Score,Title,Body
                        //Id,OwnerUserId,CreationDate,ParentId,  Score,     ,Body
                        sj.add("Id:" + record.get("Id"));
                        sj.add("CreationDate:" + record.get("CreationDate"));
                        sj.add("Score:" + record.get("Score"));
                        sj.add("FilePath:" + currentFilepath);
                        sj.add("Body:" + record.get("Body").replace("\n", "").replace("\r", ""));
                    } catch (IllegalArgumentException ex) {
                        Logger.getLogger(CSVParser.class.getName()).log(Level.INFO, "Skipping out of format file: {0}", currentFilepath);
                        if (!fileQueue.isEmpty()) {
                            initializeReaders();
                        } else {
                            everythingParsed = true;
                            break;
                        }
                    }

                    csvStrings.add(sj.toString());
                } // end of for loop
                Runtime.getRuntime().gc();
                Runtime.getRuntime().runFinalization();

                if (firstParse) {
                    firstParse = false;
                    firstRunWait.signal();
                    targetNrDocuments = i;
                    LOGGER.log(Level.FINEST, "Every loop will process {0} lines", i);
                }

                try {
                    LOGGER.finest("Queued more CSV documents. parseMore.await()");
                    parseMore.await();
                } catch (InterruptedException ex) {
                    Logger.getLogger(CSVParser.class.getName()).log(Level.SEVERE, null, ex);
                }

            } finally {
                lock.unlock();
            }

        LOGGER.log(Level.FINEST, "Parsed all files, all lines");

        lock.lock();
        try {
            try {
                LOGGER.log(Level.FINEST, "Waiting for kill.");
                parsingdone.await();
            } catch (InterruptedException ex) {
                LOGGER.log(Level.FINEST, "Killed");
            }
        } finally {
            lock.unlock();
        }

    }

    /**
     * Try to poll a list of csv strings
     * @return
     */
    public ConcurrentLinkedQueue<String> poll() {

        ConcurrentLinkedQueue<String> aux;

        lock.lock();
        try {
            try {
                if (firstParse) {
                    firstRunWait.await();
                }
            } catch (InterruptedException ex) {
                System.out.println("Signal sem await?");
                Logger.getLogger(CSVParser.class.getName()).log(Level.SEVERE, null, ex);
            }

            aux = csvStrings;
            csvStrings = new ConcurrentLinkedQueue<>();

            if (everythingParsed) {
                everythingPooled = true;
            }

            parseMore.signal();

        } finally {
            lock.unlock();
        }

        return aux;

    }

    /**
     * Retrieves the flag that indicates if every csv strings were polled 
     * @return
     */
    public synchronized boolean isEmpty() {
        return everythingPooled;
    }
    
    /**
     * Initialize the CSV readers
     */
    private void initializeReaders() {
        try {
            currentFilepath = fileQueue.poll();
            FileReader fr = new FileReader(currentFilepath);
            BufferedReader br = new BufferedReader(fr);
            Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(br);
            iterator = records.iterator();

        } catch (IOException ex) {
            Logger.getLogger(CSVParser.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
