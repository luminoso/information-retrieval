package pt.ua.deti.ir.Tokenizer;

/**
 * This class provides an encapsulation of the Tartarus Snowball PorterStemmer.
 * This stemmer is used at the tokenization process.
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class PorterStemmer implements Stemmer {

    private final org.tartarus.snowball.ext.PorterStemmer porterStemmer;

    /**
     * Initialize a PorterStemmer
     */
    public PorterStemmer() {
        porterStemmer = new org.tartarus.snowball.ext.PorterStemmer();
    }

    /* (non-Javadoc)
     * @see pt.ua.deti.ir.Tokenizer.Stemmer#stem(java.lang.String)
     */
    @Override
    public String stem(String word) {
        try {
            porterStemmer.setCurrent(word);
            porterStemmer.stem();
            return porterStemmer.getCurrent();
        } catch (Exception e) {
            return word;
        }
    }
}
