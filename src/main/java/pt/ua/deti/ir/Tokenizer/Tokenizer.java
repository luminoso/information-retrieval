package pt.ua.deti.ir.Tokenizer;

import org.apache.commons.lang3.tuple.ImmutablePair;
import pt.ua.deti.ir.Structures.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static pt.ua.deti.ir.Constants.ASCII_WORD_REGEX_MATCH;
import static pt.ua.deti.ir.Constants.MINIMUM_WORD_LENGTH;

/**
 * Tokenizes the documents words
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Tokenizer {

    private final Filter stopWords;

    /**
     * Initialize a Tokenizer
     * @param filter
     */
    public Tokenizer(Filter filter) {
        this.stopWords = filter;
    }
    
    /**
     * Tokenize the words on the provided document
     * @param doc
     */
    private void tokenize(Document doc) {
        doc.setContent(
                doc.getContent().stream()
                        .filter(s -> !s.isEmpty())
                        .filter(s -> s.length() >= MINIMUM_WORD_LENGTH)
                        .filter(s -> !stopWords.filter(s))
                        .filter(s -> s.matches(ASCII_WORD_REGEX_MATCH))
                        .map((t) ->
                        {
                            // Stemmer is NOT thread-safe! must initialize
                            Stemmer internalStemmer = new PorterStemmer();
                            return internalStemmer.stem(t);
                        })
                        .collect(Collectors.toList())
        );
    }

    /**
     * Tokenize each content for every document on the provided document list
     * @param doclist
     */
    public void tokenize(List<Document> doclist) {
        doclist.parallelStream().forEach(this::tokenize);
    }

    /**
     * Tokenize a query
     * @param query
     * @return
     */
    public ArrayList<ImmutablePair<String, String>> tokenizeQueryPair(String query) {
        Pattern id_content = Pattern.compile(ASCII_WORD_REGEX_MATCH); // pattern is thread safe

        ArrayList<String> content = new ArrayList<>();

        ArrayList<ImmutablePair<String, String>> newcontent;

        content.addAll(Arrays.asList(query.split(" ")));

        newcontent = (ArrayList<ImmutablePair<String, String>>) content.parallelStream()
                .distinct()
                .filter(s -> !s.isEmpty())
                .filter(s -> s.length() >= MINIMUM_WORD_LENGTH)
                .map(id_content::matcher)
                .filter(Matcher::find)
                .map((t) ->
                {
                    ArrayList<String> arrayList = new ArrayList<>();
                    arrayList.add(t.group(1));
                    while (t.find()) {
                        arrayList.add(t.group(1));
                    }
                    return arrayList;
                })
                .flatMap(Collection::stream)
                .filter(s -> !stopWords.filter(s))
                .map((s) ->
                {
                    // Stemmer is NOT thread-safe! must initialize
                    Stemmer internalStemmer = new PorterStemmer();
                    return new ImmutablePair<>(s, internalStemmer.stem(s));
                })
                .collect(Collectors.toList());

        return newcontent;
    }
}
