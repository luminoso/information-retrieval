package pt.ua.deti.ir.Coordinator;

import pt.ua.deti.ir.Indexer.Indexer;
import pt.ua.deti.ir.Reader.CorpusReader;
import pt.ua.deti.ir.Structures.Document;
import pt.ua.deti.ir.Tokenizer.Tokenizer;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Pipeline is a thread that runs a set of documents though the processing pipeline, which
 * includes reading and parsing the corpus, tokenization, stemming and indexing
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
class Pipeline implements Runnable {

    private final ConcurrentLinkedQueue<String> stringsQueue;      // reference where to poll more work from
    private final CorpusReader reader;
    private final Tokenizer tokenizer;
    private final Indexer indexer;
    private PipelineState state;        // state of the thread
    private int processed_documents;    // counter of processed documents
    private boolean up;                 // if true, keep thread alive and running
    private List<Document> doclist;

    /**
     * Initializes a pipeline
     * @param stringsQueue Queue with documents flattened in a single string line
     * @param reader Corpus Reader to use
     * @param tokenizer Tokenizer to use
     * @param indexer Indexer to use
     */
    public Pipeline(ConcurrentLinkedQueue<String> stringsQueue,
                    CorpusReader reader,
                    Tokenizer tokenizer,
                    Indexer indexer) {
        this.stringsQueue = stringsQueue;
        this.tokenizer = tokenizer;
        this.indexer = indexer;
        this.reader = reader;
        this.state = PipelineState.IDLE;

        processed_documents = 0;
        up = true;
    }

    @Override
    public void run() {
        state = PipelineState.IDLE;

        while (up) {
            /*
              Process the pipeline
             */
            switch (state) {
                case IDLE:
                    this.state = PipelineState.ReadingCorpus;
                    break;
                case ReadingCorpus:
                    doclist = reader.parse(stringsQueue);
                    if (doclist == null) {
                        // skip because of bad encoding or something
                        state = PipelineState.IDLE;
                        this.up = false;
                        break;
                    }
                    state = PipelineState.Tokenizing;
                    break;
                case Tokenizing:
                    tokenizer.tokenize(doclist);
                    state = PipelineState.Indexing;
                    break;
                case Indexing:
                    indexer.index(doclist);

                    processed_documents += doclist.size();
                    indexer.setProcessed_docs(indexer.getProcessed_docs() + processed_documents);

                    state = PipelineState.IDLE;
                    this.up = false;
                    break;
                case FAIL:
                    break;
                default:
                    break;
            }
        }
    }

}
