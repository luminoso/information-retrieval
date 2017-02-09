package pt.ua.deti.ir.Ranker;

import pt.ua.deti.ir.Structures.CorpusStatistics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Ranks the query results by performing IDF normalization (note that LNC was pre-computed during indexation)
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Ranker {

    private final CorpusStatistics cs;

    /**
     * Initializes a Ranker based of the corpus statistics built on indexing time
     * @param cs Corpus statistics
     */
    public Ranker(CorpusStatistics cs) {
        this.cs = cs;
    }

    /**
     * Given a map containing the results of a query, rank the result based on their weight
     * @param res Query results
     * @return Query results ranked
     */
    public ConcurrentSkipListMap<Integer, Double> rankResults(ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> res) {
        ConcurrentHashMap<String, Double> idfs = new ConcurrentHashMap<>();

        // calculate IDFs
        res.entrySet()
                .parallelStream()
                .forEach((t) ->
                        idfs.put(t.getKey(), Math.log10((double) cs.getCorpusCount() / t.getValue().size())));

        /*
         * calculate IDF normalization
         * Normalization = √( ∑ idf(tokens)² )
         */
        double normalization = Math.sqrt(idfs.reduceEntriesToDouble(0, v -> Math.pow(v.getValue(), 2), 0, Double::sum));

        ConcurrentSkipListMap<Integer, Double> scores = new ConcurrentSkipListMap<>();

        res.entrySet()
                .forEach((entry) -> // entry is token:(docid:lnc)
                        entry.getValue()
                                .entrySet()
                                .parallelStream()
                                .forEach((t) -> // docid:lnc
                                {
                                    /*
                                      Score for a given document is: Score = ∑ (for
                                      each token) = lnc * idf(token) /
                                      normalization
                                     */
                                    Double lnc = res.get(entry.getKey()).get(t.getKey());
                                    Double idf = idfs.get(entry.getKey());

                                    scores.putIfAbsent(t.getKey(), 0.0);
                                    scores.compute(t.getKey(), (k, v) -> Double.sum(v, (lnc * idf) / normalization));
                                }));

        return scores;

    }

}
