package pt.ua.deti.ir.Indexer;

import pt.ua.deti.ir.Structures.Concurrent.ConcurrentTermMap;
import pt.ua.deti.ir.Structures.CorpusStatistics;
import pt.ua.deti.ir.Structures.Document;
import pt.ua.deti.ir.Utils.Disk.DiskManager;
import pt.ua.deti.ir.Utils.Mem.MemUtil;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

/**
 * Indexing is the process where the tokens are normalized using the LNC approach and saved to disk.
 * Due to dynamic memory resources that are available in different hosts serialization can split the files according
 * to the available memory using the following strategy:
 * 1. Try to index first char
 * 2. Does it fit? Good. Write to disk. (file a, b, c, etc..)
 * 3. Memory exhausted? Split first char in a 2nd level ( aa, ab, ac, ad, ...)
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Indexer {

    private final ConcurrentTermMap tmap = new ConcurrentTermMap();
    private final int splitLevel;
    //diskmgr bits
    private final DiskManager dm;
    private int processed_docs;

    /**
     * Initializes the Indexer
     * @param dirname directory name to where to write the index files
     */
    public Indexer(String dirname) {
        processed_docs = 0;
        dm = new DiskManager(dirname);

        if (MemUtil.getMaxMem(Runtime.getRuntime()) >= 910) {
            splitLevel = 1;
        } else {
            splitLevel = 2;
        }
    }

    /**
     * Processes a document and adds it to the indexes to the merge queue
     *
     * @param doc to process
     */
    private void index(Document doc) {
        tmap.add(doc.getContent(), doc.getDocID());
        tmap.computeLNC(doc.getContent(), doc.getDocID());
    }
    
    /**
     * Processes a list of documents and adds them to the indexes to the merge queue 
     * @param doclist to process
     */
    public void index(List<Document> doclist) {
        doclist.parallelStream().forEach(this::index);
        tmap.resetIDFaux();
    }
    
    /**
     * Retrieves the number of processed documents by the indexer
     * @return
     */
    public int getProcessed_docs() {
        return processed_docs;
    }

    /**
     * Sets the number of processed documents by the indexer
     * @param processed_docs
     */
    public void setProcessed_docs(int processed_docs) {
        this.processed_docs = processed_docs;
    }

    /*
      HERE BE DRAGONS!!
     */

    /**
     * This method writes this class ConcurrentTermMap into disk, in a
     * serialized way, splitting the terms in it into different files, grouped
     * by their first letter, emptying the map completely. It does not provide a
     * way to control how much of the map we are writing to disk
     */
    public void save() {
        if (tmap.getTmap().isEmpty()) {
            return;
        }
        tmap.getTmap().entrySet()
                .parallelStream()
                .collect(Collectors.groupingByConcurrent(e -> e.getKey().substring(0, ((IntSupplier) () ->
                {
                    if (e.getKey().length() > 1) {
                        return splitLevel;
                    } else {
                        return 1;
                    }
                }).getAsInt()).replace(".", "").toLowerCase()))
                .entrySet()
                .forEach(entry ->
                {
                    if (entry != null) {
                        String fileName = entry.getKey() + ".termMap." + (dm.dirCountContains(entry.getKey()));

                        //remove the entries (from tmap) that we are about to write into disk
                        entry.getValue().parallelStream()
                                .forEach((t) -> tmap.remove(t.getKey()));

                        ConcurrentTermMap map = new ConcurrentTermMap();

                        entry.getValue().parallelStream()
                                .forEach(e -> map.put(e.getKey(), e.getValue()));

                        dm.write(fileName, map.getTmap());
                    }
                });
    }
    
    /**
     * Saves a ConcurrentTermMap to disk (by using the DiskManager)
     * @param tmap
     */
    private void save(ConcurrentTermMap tmap) {
        if (tmap.getTmap().isEmpty()) {
            return;
        }
        tmap.getTmap().entrySet()
                .parallelStream()
                .collect(Collectors.groupingByConcurrent(e -> e.getKey().substring(0, ((IntSupplier) () ->
                {
                    if (e.getKey().length() > 1) {
                        return 2;
                    } else {
                        return 1;
                    }
                }).getAsInt()).replace(".", "").toLowerCase()))
                .entrySet()
                .forEach(entry ->
                {
                    if (entry != null) {
                        String fileName = entry.getKey() + ".termMap.part." + (dm.dirCountContains(entry.getKey()));

                        //remove the entries (from tmap) that we are about to write into disk
                        entry.getValue().parallelStream()
                                .forEach((t) -> tmap.remove(t.getKey()));

                        ConcurrentTermMap map = new ConcurrentTermMap();

                        entry.getValue().parallelStream()
                                .forEach(e -> map.put(e.getKey(), e.getValue()));

                        dm.write(fileName, map.getTmap());
                    }
                });
    }
    
    /**
     * Index partitions merge that uses a dynamic multi level term map splitting based on the current memory and overall size of the complete master term map
     * It writes the resulting term maps into disk
     * @param cs
     */
    @SuppressWarnings("unchecked")
	public void mergeCrazy(CorpusStatistics cs) {
        //merge
        dm.getRFiles()
                .parallelStream()
                .filter(e -> !e.getName().contains("termMap.master"))
                .filter(e -> !e.getName().contains("termMap.part"))
                .filter(e -> e.getName().contains("termMap."))
                .collect(Collectors.groupingByConcurrent(e -> e.getName().substring(0, ((IntSupplier) () ->
                {
                    if (e.getName().length() > 1) {
                        return splitLevel;
                    } else {
                        return 1;
                    }

                }).getAsInt()).replace(".", "").toLowerCase()))
                .entrySet()
                .forEach(filelist ->
                {
                    AtomicLong size = new AtomicLong();

                    ConcurrentTermMap master = new ConcurrentTermMap();
                    AtomicBoolean todisk = new AtomicBoolean(false);
                    filelist.getValue()
                            .forEach(file ->
                            {
                                if (MemUtil.getMaxMem(Runtime.getRuntime()) <= 910) {
                                    if (MemUtil.toMB(size.addAndGet(file.length())) * 10 >= MemUtil.getFreeMem(Runtime.getRuntime())) {
                                        //no space
                                        save(master);
                                        master.getTmap().entrySet().parallelStream().forEach(entry -> master.remove(entry.getKey()));
                                        todisk.set(true);
                                        Runtime.getRuntime().gc();
                                        Runtime.getRuntime().runFinalization();
                                    }
                                }

                                ((ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>>) dm.read(file.getPath())).entrySet()
                                        .parallelStream()
                                        .forEach((t) ->
                                                master.putMaster(t.getKey(), t.getValue()));

                                if (file.exists()) {
                                    dm.rfilesRemove(file.getName());
                                    file.delete();
                                }
                            });
                    if (todisk.get()) {
                        save(master);
                        master.getTmap().clear();
                        Runtime.getRuntime().gc();
                        Runtime.getRuntime().runFinalization();
                        merge();
                    } else {
                        File f = new File(dm.dirname + "/" + filelist.getKey() + ".termMap.master");
                        if (f.exists()) {
                            dm.rfilesRemove(f.getName());
                            f.delete();
                        }
                        //increment token count before saving
                        cs.incTokenCount(master.getTokenCount());

                        dm.write(filelist.getKey() + ".termMap.master", master.getTmap());
                    }

                });
    }
    
    /**
     * Index partitions merge with a fixed splitting level
     * It writes the resulting term maps into disk
     */
    @SuppressWarnings("unchecked")
    private void merge() {
        //merge
        dm.getRFiles()
                .parallelStream()
                .filter(e -> e.getName().contains("termMap.part."))
                .collect(Collectors.groupingByConcurrent(e -> e.getName().substring(0, ((IntSupplier) () ->
                {
                    if (e.getName().length() > 1) {
                        return 2;
                    } else {
                        return 1;
                    }

                }).getAsInt()).replace(".", "").toLowerCase()))
                .entrySet()
                .forEach(filelist ->
                {
                    ConcurrentTermMap master = new ConcurrentTermMap();

                    filelist.getValue()
                            .forEach(file ->
                            {

                                ((ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>>) dm.read(file.getPath())).entrySet()
                                        .parallelStream()
                                        .forEach(termEntry ->
                                        {
                                            // key: Token; Value: HashMap
                                            termEntry.getValue()
                                                    .entrySet()
                                                    .parallelStream()
                                                    .forEach(docIDEntry
                                                            -> // for each token add an document
                                                            master.add(termEntry.getKey(), docIDEntry.getKey())
                                                    );
                                        });

                                if (file.exists()) {
                                    dm.rfilesRemove(file.getName());
                                    file.delete();
                                }
                            });

                    File f = new File(dm.dirname + "/" + filelist.getKey() + ".termMap.master");
                    if (f.exists()) {
                        dm.rfilesRemove(f.getName());
                        f.delete();
                    }
                    dm.write(filelist.getKey() + ".termMap.master", master.getTmap());
                });
    }

}
