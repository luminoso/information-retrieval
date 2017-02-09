package pt.ua.deti.ir;

import org.apache.commons.cli.*;
import pt.ua.deti.ir.Coordinator.Coordinator;
import pt.ua.deti.ir.Search.SearchCLI;

import java.util.Scanner;

/**
 * Main class of IR engine
 *
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class Main {

	/**
	 * Main class entry point
	 * @param args
	 */
    public static void main(String[] args) {
        // process command line arguments
        // set up our flags
        Options options = new Options();
        options.addOption("h", false, "print the help message");
        options.addOption("d", true, "corpus directory");
        options.addOption("f", true, "filter list");
        options.addOption("o", true, "output directory path");
        options.addOption("q", false, "query database for keywords (search)");

        // generate help
        HelpFormatter formatter = new HelpFormatter();

        //Parse the arguments
        CommandLineParser cliParser = new DefaultParser();
        CommandLine cmd;

        //Default values
        String directory = "./stacksample";
        String filterListPath = "./stop_processed.txt";
        String outputPath = "./disk";
        Boolean queryMode = false;

        try {
            cmd = cliParser.parse(options, args);
            for (Option o : cmd.getOptions()) {
                switch (o.getOpt()) {
                    case "q":
                        queryMode = true;
                        break;
                    case "d":
                        directory = o.getValue();
                        break;
                    case "f":
                        filterListPath = o.getValue();
                        break;
                    case "o":
                        outputPath = o.getValue();
                        break;
                    case "help":
                        formatter.printHelp(" ", options);
                        return;
                    default:
                        formatter.printHelp(" ", options);
                        break;
                }
            }
        } catch (ParseException e) {
            System.out.println("Wrong arguments. Try -h");
            System.exit(0);
        }

        Log.init();

        if (queryMode) {
            SearchCLI search = new SearchCLI(outputPath, filterListPath);
            do {
                System.out.print("Insert query (Control+c to exit): ");
                Scanner sc = new Scanner(System.in);
                String query = sc.nextLine();

                System.out.print("Number of results to query (10): ");
                sc = new Scanner(System.in);
                String results = sc.nextLine();

                int numberOfResults = 10;
                try {
                    numberOfResults = Integer.parseInt(results);
                } catch (NumberFormatException ex) {
                    //
                }

                search.query(query, numberOfResults);

            } while (true);

        } else {
            // if we are processing data...
            Coordinator cord = new Coordinator(directory, filterListPath, outputPath);
            cord.initiateProcess();
        }
    }
}
