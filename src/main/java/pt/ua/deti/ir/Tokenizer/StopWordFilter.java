package pt.ua.deti.ir.Tokenizer;

import java.util.Set;

/**
 * The StopWordFilter is a particular implementation of the Filter.
 * It is used to filter stop words before they are stemmed at the tokenization process.
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class StopWordFilter implements Filter {

    private final Set<String> stopWords;

    /**
     * Filter a set of string
     * @param stopWords
     */
    public StopWordFilter(Set<String> stopWords) {
        this.stopWords = stopWords;
    }

    @Override
    public boolean filter(String word) {
        return stopWords.contains(word);
    }
}
