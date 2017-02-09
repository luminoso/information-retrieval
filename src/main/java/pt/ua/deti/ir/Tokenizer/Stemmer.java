package pt.ua.deti.ir.Tokenizer;

/**
 * Stemmer interface is used to abstract the stemmer objects used on the tokenization process
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
interface Stemmer {

    /**
     * A stem method
     * @param word string to be stemmed
     * @return
     */
    String stem(String word);

}
