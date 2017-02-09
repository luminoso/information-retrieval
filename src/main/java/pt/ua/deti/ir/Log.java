package pt.ua.deti.ir;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Log class
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Log {

    private final static Logger LOGGER = Logger.getLogger(Log.class.getName());

    /**
     * Initialize the logger
     */
    public static void init() {

        ConsoleHandler handler = new ConsoleHandler();
        // PUBLISH this level
        handler.setLevel(Level.FINEST);
        LOGGER.addHandler(handler);
        LOGGER.setLevel(Level.FINEST);
    }

}
