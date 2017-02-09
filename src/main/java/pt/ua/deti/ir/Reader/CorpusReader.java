package pt.ua.deti.ir.Reader;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import pt.ua.deti.ir.Structures.Document;
import pt.ua.deti.ir.Utils.Disk.DiskManager;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static pt.ua.deti.ir.Constants.CORPUS_REGEX_DOCUMENT;

/**
 * The purpose of Corpus Reader is to create the initial document structure
 * associated to a file. 1. Parsing the document striping unnecessary
 * tags/symbols/letters 2. Fill the Document structure with data 3. Return the
 * Document
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class CorpusReader {

    private final AtomicInteger corpusCount = new AtomicInteger(0);
    private final DiskManager dm;
    private ConcurrentHashMap<Integer, Pair<String, Integer>> corpusIDToPath = new ConcurrentHashMap<>();

    public CorpusReader(String dirname) {
        dm = new DiskManager(dirname);
    }

    /**
     * Parses all document present in the referenced file path
     *
     * @param stringsQueue to parse
     * @return list with all documents with it's content in untokenized/unstemmed raw keywords
     */
    public List<Document> parse(ConcurrentLinkedQueue<String> stringsQueue) {

        //compile our corpus regex so we can apply it on our parsing process
        Pattern id_content = Pattern.compile(CORPUS_REGEX_DOCUMENT);

        //parsing process
        return stringsQueue.parallelStream()
                .filter(line -> !line.isEmpty()) // line is not empty
                .map(id_content::matcher)// regex it
                .filter(Matcher::find) // did we regex anything? if so create document
                .map(match ->
                {
                    //get the corpusID for this new file that we processing
                    int corpusID = corpusCount.getAndIncrement();

                    //map the corpusID to its corresponding filepath
                    corpusIDToPath.computeIfAbsent(corpusID, v -> new ImmutablePair<>(match.group(4), Integer.parseInt(match.group(1))));
                    return new Document(
                            corpusID, //first match is doc id and used to create our own doc id
                            Arrays.asList(match.group(5).split(" ")).parallelStream() // split document content in words
                                    .collect(Collectors.toList())); // and put them in a list
                })
                .collect(Collectors.toList()); //collect all parsed lines
    }
    
    /**
     * Getter for the corpusCount - counts the number of corpus read
     * @return
     */
    public int getCorpusCount() {
        return corpusCount.get();
    }

    public synchronized void save() {
        if (corpusIDToPath.isEmpty()) {
            return;
        }

        String fileName = corpusCount.get() + ".docMap";

        dm.write(fileName, corpusIDToPath);

        corpusIDToPath = new ConcurrentHashMap<>();

        Runtime.getRuntime().gc();
        Runtime.getRuntime().runFinalization();
    }
}
