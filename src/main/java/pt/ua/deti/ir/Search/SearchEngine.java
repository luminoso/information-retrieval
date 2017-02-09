package pt.ua.deti.ir.Search;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import pt.ua.deti.ir.Constants;
import pt.ua.deti.ir.Coordinator.Coordinator;
import pt.ua.deti.ir.Structures.Concurrent.ConcurrentTermMap;
import pt.ua.deti.ir.Structures.CorpusStatistics;
import pt.ua.deti.ir.Tokenizer.Filter;
import pt.ua.deti.ir.Tokenizer.StopWordFilter;
import pt.ua.deti.ir.Tokenizer.Tokenizer;
import pt.ua.deti.ir.Utils.Disk.DiskManager;
import pt.ua.deti.ir.Utils.Disk.DiskUtils;
import pt.ua.deti.ir.Utils.Mem.MemMgr;
import pt.ua.deti.ir.Utils.Mem.MemUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * 
 * Provides a search engine over the index maps built during the indexing phase,
 * it uses a dynamic cache for this purpose
 * 
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
class SearchEngine {
    private final ConcurrentSkipListMap<String, Integer> splitTable;
    private final Tokenizer tkzer;
    //key -> timestamp, term map
    private final ConcurrentSkipListMap<String, MutablePair<Long, ConcurrentTermMap>> tmap_cache;
    //docID -> timestamp, docMap Path, docMap
    private final ConcurrentSkipListMap<Integer, MutableTriple<Long, String, ConcurrentHashMap<Integer, Pair<String, Integer>>>> dmap_cache;
    private final DiskManager dm;
    private final MemMgr mem;
    private final String dirname;

    /**
     * Initialize a SearchEngine
     * @param dirname the path where the indexer output is
     * @param stopwordspath filter list filepath used for filtering queries
     */
    public SearchEngine(String dirname, String stopwordspath) {
        this.dirname = dirname;

        dm = new DiskManager(dirname);
        mem = new MemMgr();
        Thread memT = new Thread(mem);
        memT.start();

        tmap_cache = new ConcurrentSkipListMap<>();

        dmap_cache = new ConcurrentSkipListMap<>();
        //since dmaps holds a mapping docID interval -> docID filepath we can afford to load this map in to mem right now
        buildDmap_cache();

        HashSet<String> stopWords = new HashSet<>();
        try {
            stopWords = DiskUtils.retrieveFilterList(stopwordspath);
        } catch (IOException | NullPointerException ex) {
            Logger.getLogger(Coordinator.class.getName()).log(Level.SEVERE, null, ex);
        }

        //despite this not being thread-safe, the Tokenizer is accessed in a synchronized fashion
        Filter filter = new StopWordFilter(stopWords);
        tkzer = new Tokenizer(filter);

        //build our split table
        splitTable = new ConcurrentSkipListMap<>();
        buildSplitTable();
    }

    /**
     * Searches for a token within the index
     * @param token the token
     * @return a mapping between the found token and document IDs
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> search(String token) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> mapping = new ConcurrentHashMap<>();

        int level = checkSplitLevel(token);

        if (tmap_cache.entrySet().stream().sequential().noneMatch(entry -> entry.getKey().equals(token.substring(0, level)))) {
            //tmap not loaded - we need to load the correspondent map to search for matching docIDs
            String l_idx = token.substring(0, level);
            File toLoad = new File(dirname + "/" + l_idx + ".termMap.master");

            if (!toLoad.exists()) {
                //the token we are searching for is not mapped in disk, so we return an empty map
                return mapping;
            }
            long fileSize = toLoad.length();

            double used = mem.getUsedPer();
            double max = mem.getMaxMem();
            long fSizeMB = MemUtil.toMB(fileSize);
            double inflateRate = 2.0;
            double inflated = fSizeMB * inflateRate / max;
            double toCompare = used + inflated;

            while (toCompare >= 0.60 && !tmap_cache.isEmpty()) {
                timeOutTMap();
                System.runFinalization();
                System.gc();
            }
            loadTMap(l_idx, toLoad.getAbsolutePath());
        }

        //list of doc IDs where the term occurs
        //retrieve the token termMap
        ConcurrentHashMap<Integer, Double> docIDs = tmap_cache.get(token.substring(0, level))
                .getRight()
                .getTmap()
                .get(token);

        if (docIDs == null) {
            //the token we are searching for is not mapped in memory, so we return an empty map
            return mapping;
        }

        //update the timestamp
        tmap_cache.get(token.substring(0, level))
                .setLeft(System.currentTimeMillis());

        //insert said list into a mapping data structure
        mapping.put(token, docIDs);

        return mapping;
    }

    /**
     * Tokenizes a query string
     *
     * @param query
     * @return
     */
    public synchronized ArrayList<ImmutablePair<String, String>> splitQuery(String query) {
        return tkzer.tokenizeQueryPair(query);
    }

    /**
     * Get the closest docMap ID to the given docID
     *
     * @param docID
     * @return
     */
    private Integer getClosestDocumentMapID(int docID) {
        return dmap_cache.ceilingKey(docID);
    }

    /**
     * Query the document map cache for a document ID
     * @param docID
     * @return
     */
    private String getDocumentMapPath(int docID) {
        return dmap_cache.ceilingEntry(docID).getValue().getMiddle();
    }

    /**
     * Find and retrieve the document map based on the document ID
     * @param docID
     * @return
     */
    public String getDocumentMap(int docID) {
        String path = getDocumentMapPath(docID);
        int closestID = getClosestDocumentMapID(docID);

        long fileSize = new File(path).length();

        double used = mem.getUsedPer();
        double max = mem.getMaxMem();
        long fSizeMB = MemUtil.toMB(fileSize);
        double inflateRate = 2.5;
        double inflated = fSizeMB * inflateRate / max;
        double toCompare = used + inflated;

        while (toCompare >= 0.50 && !dmap_cache.entrySet().parallelStream().allMatch(entry -> entry.getValue().getRight() == null)) {
            timeOutDMap();
            System.runFinalization();
            System.gc();
        }
        loadDMap(closestID);

        return dmap_cache.get(closestID).getRight().get(docID).getLeft();

    }

    /**
     * Read and unserialize the corpus statistics file into memory
     * @return
     */
    public CorpusStatistics getCorpusStatistics() {
        return (CorpusStatistics) dm.read(new File(dirname + "/" + Constants.STATS_FILE).getAbsolutePath());
    }

    /**
     * Timeout and remove from cache the oldest term map
     */
    private void timeOutTMap() {

        //remove the tmap with the oldest timestamp
        String key = tmap_cache.entrySet()
                .parallelStream()
                .filter(Objects::nonNull)
                .filter(entry -> entry.getValue() != null)
                .min(Comparator.comparingLong(map -> map.getValue().getLeft()))
                .get()
                .getKey();

        tmap_cache.remove(key);
    }

    /**
     * Given a file path, load a TermMap into memory (into the tmaps object)
     *
     * @param filepath
     */
    @SuppressWarnings("unchecked")
    private void loadTMap(String key, String filepath) {
        tmap_cache.put(key, new MutablePair<>(System.currentTimeMillis(), new ConcurrentTermMap((ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>>) dm.read(filepath))));
    }

    /**
     * Timeout and remove from cache the oldest document map
     */
    private void timeOutDMap() {

        Integer docID = dmap_cache.entrySet()
                .parallelStream()
                .filter(entry -> entry.getValue().getLeft() != null && entry.getValue().getRight() != null)
                .min(Comparator.comparingLong(map -> map.getValue().getLeft()))
                .get()
                .getKey();

        dmap_cache.get(docID).setLeft(null);
        dmap_cache.get(docID).setRight(null);
    }

    /**
     * Given a key, it loads one docMap into memory
     *
     * @param key
     */
    @SuppressWarnings("unchecked")
    private void loadDMap(int key) {
        String path = dmap_cache.get(key).getMiddle();

        dmap_cache.put(key, new MutableTriple<>(System.currentTimeMillis(), path, (ConcurrentHashMap<Integer, Pair<String, Integer>>) dm.read(path)));
    }

    /**
     * Builds the docMap cache map
     */
    private void buildDmap_cache() {
        dm.getRFiles()
                .parallelStream()
                .filter(file -> file.getName().contains("docMap"))
                .forEach(file -> dmap_cache.put(Integer.valueOf(file.getName().split("\\.")[0]), new MutableTriple<>(System.currentTimeMillis(), file.getPath(), null)));
    }

    /**
     * Build an in-memory mapping between the filenames of on-disk master term maps and their filename lengths.
     * Why you ask? Because we need be able to decide on how to split a term to find its on-disk term map efficiently.
     */
    private void buildSplitTable() {
        dm.getRFiles()
                .parallelStream()
                .filter(file -> file.getName().contains("termMap.master"))
                .map(file -> file.getName().split("\\.")[0])
                .collect(Collectors.toCollection(ConcurrentSkipListSet::new))
                .parallelStream()
                .forEach(filename ->
                        splitTable.put(filename, filename.length()));
    }

    /**
     * Using the previously built splitTable, find the split level of a term.
     *
     * @param term
     * @return
     */
    private int checkSplitLevel(String term) {
        long hits = splitTable.entrySet().parallelStream().filter(entry -> entry.getKey().charAt(0) == term.charAt(0)).count();

        if (hits == 1) {
            return 1;
        } else if (hits > 1) {
            return dm.getRFiles().parallelStream()
                    .map(File::getName)
                    .filter(filename -> filename.charAt(0) == term.charAt(0))
                    .collect(Collectors.toList())
                    .get(0)
                    .split("\\.")[0]
                    .length();
        }
        return 0;
    }

}
