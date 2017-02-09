package pt.ua.deti.ir.Search;

import de.vandermeer.asciitable.v2.V2_AsciiTable;
import de.vandermeer.asciitable.v2.render.V2_AsciiTableRenderer;
import de.vandermeer.asciitable.v2.render.WidthFixedColumns;
import de.vandermeer.asciitable.v2.themes.V2_E_TableThemes;
import org.apache.commons.lang3.tuple.ImmutablePair;
import pt.ua.deti.ir.Ranker.Ranker;
import pt.ua.deti.ir.Structures.CorpusStatistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

/**
 * CLI (Command Line Interface) for querying the database.
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class SearchCLI {
    private final SearchEngine engine;

    /**
     * Initializes a SearchCLI
     * @param outputPath the path where the indexer output is
     * @param filterListPath filter list filepath used for filtering queries
     */
    public SearchCLI(String outputPath, String filterListPath) {
        this.engine = new SearchEngine(outputPath, filterListPath);
    }

    /**
     * Perform a query
     * @param query the query as a string
     * @param resultsSize the amount of results to display
     */
    public void query(String query, int resultsSize) {
        ArrayList<ImmutablePair<String, String>> squery = engine.splitQuery(query);

        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Double>> res = new ConcurrentHashMap<>();

        if (!squery.isEmpty())
            squery.forEach(e -> res.putAll(engine.search(e.getRight())));

        CorpusStatistics corpusStatistics = engine.getCorpusStatistics();
        Ranker ranker = new Ranker(corpusStatistics);
        ConcurrentSkipListMap<Integer, Double> sortResults = ranker.rankResults(res);

        V2_AsciiTable at = new V2_AsciiTable();

        at.addRule();
        at.addRow(null, null, null, null, null, "Information Retrieval").setAlignment(new char[]{'c', 'c', 'c', 'c', 'c', 'c'});
        at.addRule();
        at.addRule();

        at.addRow(null, "Terms:", null, null, null, "");
        at.addRow(null, "• Query", null, null, null, squery.stream().map(t -> t.left).collect(Collectors.toList()));
        at.addRow(null, "• Tokenized", null, null, null, squery.stream().map(ImmutablePair::getRight).collect(Collectors.toList()));
        at.addRule();
        at.addRule();

        at.addRow(null, "Results found", null, null, null, sortResults.size());
        at.addRule();
        at.addRow(null, "Database size", null, null, null, corpusStatistics.getCorpusCount());
        at.addRule();
        at.addRow(null, "Token count", null, null, null, (int) corpusStatistics.getTokenCount());
        at.addRule();
        at.addRow(null, "Results to retrieve", null, null, null, resultsSize);
        at.addRule();
        at.addRule();

        at.addRow("Rank", "Score", "Document", null, null, "Path").setAlignment(new char[]{'c', 'c', 'c', 'c', 'c', 'c'});
        at.addRule();

        Iterator<Map.Entry<Integer, Double>> iterator = sortResults.entrySet().parallelStream().sorted((v1, v2) -> Double.compare(v2.getValue(), v1.getValue())).iterator();
        if (iterator.hasNext()) {
            for (int i = 0; i < resultsSize && iterator.hasNext(); i++) {
                Map.Entry<Integer, Double> next = iterator.next();
                at.addRow(i + 1, next.getValue(), next.getKey(), null, null, engine.getDocumentMap(next.getKey())).setAlignment(new char[]{'c', 'l', 'c', 'c', 'c', 'l'});
            }
        } else {
            at.addRow(null, null, null, null, null, "No results found");
        }


        at.addRule();

        V2_AsciiTableRenderer rend = new V2_AsciiTableRenderer();
        rend.setTheme(V2_E_TableThemes.UTF_LIGHT.get());
        rend.setWidth(new WidthFixedColumns().add(6).add(24).add(10).add(16).add(14).add(56));
        System.out.println(rend.render(at));

    }

}
