package pt.ua.deti.ir.Structures.Concurrent;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Term map containing for each token the docID that has the token and the respective computed LNC normalization value
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class ConcurrentTermMap {

    /**
     * Concurrent Term Map
     */
    // token : (docid: occurrence counter)
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> tmap;
    private ConcurrentHashMap<Integer, Double> lncaux = new ConcurrentHashMap<>();

    /**
     * Create an empty TermMap
     */
    public ConcurrentTermMap() {
        this.tmap = new ConcurrentHashMap<>();
    }

    /**
     * Create a TermMap from an existing term map mapping 
     * @param map
     */
    public ConcurrentTermMap(ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> map) {
        this.tmap = map;
    }

    /**
     * Insert a posting for a certain term
     *
     * @param term to add to the map
     * @param docID of the token
     */
    public void add(String term, Integer docID) {
        tmap.computeIfAbsent(term, v -> new ConcurrentHashMap<>()).merge(docID, 1d, Double::sum);
    }

    /**
     * Insert several terms belonging to a posting ID
     *
     * @param terms to add
     * @param docID of the terms
     */
    public void add(List<String> terms, Integer docID) {
        terms.forEach(term -> add(term, docID));
    }

    /**
     * Get term map in its "raw" format
     *
     * @return term map
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> getTmap() {
        return tmap;
    }

    /**
     * Remove an entry of the map based from a key
     * @param key
     */
    public void remove(String key) {
        tmap.remove(key);
    }

    /**
     * Put a value into the map given its key
     * @param key
     * @param value
     */
    public void put(String key, ConcurrentHashMap<Integer, Double> value) {
        tmap.put(key, value);
    }
    
    /**
     * Compute the LNC weights present in this map for a list of terms and a document ID
     * @param terms
     * @param docID
     */
    public void computeLNC(List<String> terms, Integer docID) {
        lncaux.putIfAbsent(docID, 0d);

        terms.stream()
                .distinct()
                .forEach((term) ->
                        tmap.get(term).compute(docID, (x, f) ->
                        {
                            Double weight = 1.0 + Math.log10(f);
                            lncaux.compute(docID, (t, u) -> u + Math.pow(weight, 2.0));
                            return weight;
                        }));

        lncaux.compute(docID, (k, v) -> Math.sqrt(v));

        terms.stream()
                .distinct()
                .forEach((term) ->
                        tmap.get(term).compute(docID, (k, v) -> v / lncaux.get(docID)));
    }
    
    /**
     * Batch insertions of documentID -> weight into the current term map given a certain key
     * @param key
     * @param value
     */
    public void putMaster(String key, ConcurrentHashMap<Integer, Double> value) {
        tmap.computeIfAbsent(key, v -> new ConcurrentHashMap<>()).putAll(value);
    }

    /**
     * Getter - token count for the current map
     */
    public int getTokenCount() {
        return tmap.size();
    }

    /**
     * Reset the aux map used for the LNC computation
     */
    public void resetIDFaux() {
        lncaux = new ConcurrentHashMap<>();
    }

}
