package pt.ua.deti.ir.Coordinator;

import pt.ua.deti.ir.Indexer.Indexer;
import pt.ua.deti.ir.Log;
import pt.ua.deti.ir.Reader.CSVParser;
import pt.ua.deti.ir.Reader.CorpusReader;
import pt.ua.deti.ir.Structures.CorpusStatistics;
import pt.ua.deti.ir.Tokenizer.Filter;
import pt.ua.deti.ir.Tokenizer.StopWordFilter;
import pt.ua.deti.ir.Tokenizer.Tokenizer;
import pt.ua.deti.ir.Utils.Disk.DiskUtils;
import pt.ua.deti.ir.Utils.Mem.MemMgr;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static pt.ua.deti.ir.Constants.CORPUS_COUNT_HINT;
import static pt.ua.deti.ir.Constants.CORPUS_FILE_EXTENSION;

/**
 * Controls the whole process of reading a corpus, tokenizing, indexing, etc
 * Contains a few tricks to force garbage collector to cleanup memory before indexation
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Coordinator {

    private final static Logger LOGGER = Logger.getLogger(Log.class.getName());
    private final String corpusPath;
    private final String filterListPath;
    private final String outputPath;
    private long startTime;
    private long threadTime;

    /**
     * Initializes a Coordinator
     * @param corpusPath path that contains corpus files
     * @param filterListPath path that contains stopwords
     * @param outputPath output path to save the serialized processed data
     */
    public Coordinator(String corpusPath, String filterListPath, String outputPath) {
        this.corpusPath = corpusPath;
        this.filterListPath = filterListPath;
        this.outputPath = outputPath;
    }

    /**
     * read directory files
     * add directory file paths to fileListQueue
     * initiate threads
     */
    @SuppressWarnings(
            {
                    "rawtypes", "unchecked"})
    public void initiateProcess() {
        HashSet<String> wordsSet = null;

        try {
            wordsSet = DiskUtils.retrieveFilterList(filterListPath);
        } catch (IOException | NullPointerException ex) {
            Logger.getLogger(Coordinator.class.getName()).log(Level.SEVERE, null, ex);
        }

        // retrieve directory, list files paths and add initialize a fileListQueue
        File documentspath = new File(corpusPath);

        ConcurrentLinkedQueue<String> fileListQueue = new ConcurrentLinkedQueue(Arrays.asList(documentspath.listFiles())
                .parallelStream()
                .map(File::getAbsolutePath)
                .filter(f -> f.endsWith(CORPUS_FILE_EXTENSION))
                .collect(Collectors.toList()));

        // mem management
        MemMgr mg = new MemMgr();
        Thread mgt = new Thread(mg);
        mgt.start();

        // pipeline bolts
        CSVParser csvparser = new CSVParser(fileListQueue, mg);
        Thread csvparsert = new Thread(csvparser);
        csvparsert.start();

        // Reasoning: csvparser is starving when pipeline is running
        csvparsert.setPriority(Thread.MAX_PRIORITY);

        CorpusReader reader = new CorpusReader(outputPath);
        Indexer indexer = new Indexer(outputPath);
        Filter filter = new StopWordFilter(wordsSet);
        Tokenizer tokenizer = new Tokenizer(filter);

        // statistics
        CorpusStatistics cs = new CorpusStatistics(outputPath);
        int processed_docs = indexer.getProcessed_docs();
        startTime = System.currentTimeMillis();

        while (!csvparser.isEmpty()) {
            LOGGER.finest("Initializing a new pipeline, doing an csvparser.poll()");
            Pipeline pipeline = new Pipeline(csvparser.poll(), reader, tokenizer, indexer);

            Thread t = new Thread(pipeline);
            threadTime = System.currentTimeMillis();
            t.start();
            LOGGER.log(Level.FINEST, "Pipeline started");

            try {
                t.join();
            } catch (InterruptedException ex) {
                Logger.getLogger(Coordinator.class.getName()).log(Level.SEVERE, null, ex);
            }

            LOGGER.log(Level.FINEST, "Pipeline ended");
            LOGGER.log(Level.FINEST, "Memory manager reports: {0}", mg.getUsedMem());
            LOGGER.info("Saving snapshot...");
            reader.save();
            indexer.save();
            Runtime.getRuntime().gc();
            Runtime.getRuntime().runFinalization();

            printLapStats(indexer.getProcessed_docs(), processed_docs, reader.getCorpusCount());
            processed_docs = indexer.getProcessed_docs();
        }

        LOGGER.log(Level.INFO, "Pipeline processed all corpus");

        long indexingElapsedTime = System.currentTimeMillis() - startTime;
        long printElapsedTime = System.currentTimeMillis() - startTime - indexingElapsedTime;

        //stop the mem manager and csvparser
        mg.setStopFlag(true);
        mgt.interrupt();
        csvparsert.interrupt();
        try {
            mgt.join();
            csvparsert.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Coordinator.class.getName()).log(Level.SEVERE, null, ex);
        }


        LOGGER.log(Level.FINEST, "Memory manager reports: {0}", mg.getUsedMem());
        cs.setCorpusCount(reader.getCorpusCount()); // saves the doc count

        //clean up some objects before merging
        csvparsert = null;
        csvparser = null;
        reader = null;
        filter = null;
        tokenizer = null;
        documentspath = null;
        fileListQueue = null;
        wordsSet = null;

        Runtime.getRuntime().gc();
        Runtime.getRuntime().runFinalization();

        LOGGER.log(Level.FINE, "Saving done. Merging");
        LOGGER.log(Level.FINEST, "Memory manager reports: {0}", mg.getUsedMem());
        indexer.mergeCrazy(cs);
        cs.save();

        LOGGER.log(Level.FINE, "Merge done");

        System.out.format("Indexing took %.2f sec\n", indexingElapsedTime / 1000.0);
        System.out.format("Writing files took %.2f sec\n", printElapsedTime / 1000.0);
        System.out.format("Processed %d documents in %.2f sec\n", indexer.getProcessed_docs(), (indexingElapsedTime + printElapsedTime) / 1000.0);
    }

    /**
     * Prints statistics every lap
     * @param processedDocs number of processed docs
     * @param lastLapProcessedDocs last lap processed docs
     * @param corpusCount current corpus count
     */
    private void printLapStats(int processedDocs, int lastLapProcessedDocs, int corpusCount) {
        long estimatedTime = System.currentTimeMillis() - threadTime;

        System.out.format("Processed %6d corpus in %6d msec. ", processedDocs - lastLapProcessedDocs, estimatedTime);

        long remainingTotalTime = ((CORPUS_COUNT_HINT - corpusCount) * (System.currentTimeMillis() - startTime)) / corpusCount;

        System.out.format("Remaining ~ %7d corpus. ETA:", CORPUS_COUNT_HINT - corpusCount);

        double secondsRemaining = remainingTotalTime * 0.001;

        if (secondsRemaining > 60) {
            System.out.format(" %2d minutes and %2d seconds %n", (int) secondsRemaining / 60, (int) secondsRemaining % 60);
        } else {
            System.out.format(" %2d seconds %n", (int) secondsRemaining);
        }
    }
}
