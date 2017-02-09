package pt.ua.deti.ir.Tokenizer;

/**
 * Filter interface is used to abstract the filtering objects used on several processes along the indexing tasks
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public interface Filter {

	/**
	 * A filtering method
	 * @param word string to be filtered
	 * @return
	 */
    boolean filter(String word);
}
