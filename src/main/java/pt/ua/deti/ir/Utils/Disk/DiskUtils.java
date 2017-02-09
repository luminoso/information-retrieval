package pt.ua.deti.ir.Utils.Disk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.nustaq.serialization.FSTObjectInput;
import pt.ua.deti.ir.Coordinator.Coordinator;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * DiskUtils provides a set of disk utility methods
 * 
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class DiskUtils {

    /**
     * Read and unserialize method for ConcurrentTermMap objects
     *
     * @param fileName
     * @return
     */
    public static Object read(String fileName) {
        Object obj = null;

        File file = new File(fileName);

        FileInputStream fin;
        FSTObjectInput ois;
        try {
            fin = new FileInputStream(file);
            ois = new FSTObjectInput(fin);

            obj = ois.readObject();

            ois.close();
            fin.close();
        } catch (IOException | ClassNotFoundException e) {
            Logger.getLogger(DiskManager.class.getName()).log(Level.SEVERE, null, e);
            System.out.println("Directory:" + fileName);
        }

        return obj;
    }


    /**
     * Takes a file and reads its lines into an ArrayList
     *
     * @param file
     * @return
     * @throws IOException
     */
    private static ArrayList<String> fileLinesToArray(File file) throws IOException {
        //Create a new FileReader
        FileReader fileReader = new FileReader(file);

        //Create a BufferedReader
        BufferedReader readBuffer = new BufferedReader(fileReader);

        //Create an ArrayList to store the contents
        ArrayList<String> lines;

        lines = new ArrayList<>(readBuffer.lines()
                .parallel()
                .map(String::trim)
                .collect(Collectors.toList()));

        //Close the reader
        readBuffer.close();
        fileReader.close();

        return lines;
    }

    /**
     * Takes a file and reads its content into a String
     *
     * @param file a java File Object
     * @return String with the content
     * @throws IOException
     */
    private static String fileContentToString(File file) throws IOException {
        //Create a new FileReader
        FileReader fileReader = new FileReader(file);

        //Create a BufferedReader
        BufferedReader readBuffer = new BufferedReader(fileReader);

        //Create a StringBuilder to store the contents
        StringBuilder stringBuilder = new StringBuilder();

        // stringbuilder is not thread safe!
        readBuffer.lines().map(s -> stringBuilder.append(s.trim()));

        //Close the reader
        readBuffer.close();
        fileReader.close();

        return stringBuilder.toString();
    }

    /**
     * Retrieve the filter list, this should method should be intelligent enough
     * to distinguished between .txt and .json files
     *
     * @param path
     * @return
     * @throws IOException
     * @throws NullPointerException
     */
    public static HashSet<String> retrieveFilterList(String path) throws IOException, NullPointerException {
        File file = new File(path);

        HashSet<String> wordsSet = null;

        if (!file.isFile()) {
            //If the file is not an actual file, throw an IOException
            throw new IOException("File is not actually a file");
        } else if (!file.canRead()) {
            //If the file cannot be written, throw an IOException
            throw new IOException("File could not be read");
        } else {
            try {
                //TODO- for better flow control this should be turned into an enum that exposes the supported file extensions
                String extension = "";
                if (path.lastIndexOf(".") != -1) {
                    extension = path.substring(path.lastIndexOf(".") + 1);
                }

                switch (extension) {
                    case "txt":
                        //txt file with content in lines
                        ArrayList<String> fileLines = fileLinesToArray(file);
                        wordsSet = new HashSet<>(fileLines);
                        break;
                    case "json":
                        //json file
                        String jsonFileString = fileContentToString(file);

                        Gson gson = new GsonBuilder().create();
                        JsonObject data = gson.fromJson(jsonFileString, JsonObject.class);
                        JsonElement list = data.get("list");

                        wordsSet = new HashSet<>();

                        for (JsonElement s : list.getAsJsonArray()) {
                            wordsSet.add(s.getAsString());
                        }
                        break;
                    case "":
                        //file with no extension
                        //TODO - not sure if needed, may be removed later
                        break;
                }

            } catch (IOException e) {
                Logger.getLogger(Coordinator.class.getName()).log(Level.SEVERE, null, e);
            }
        }

        if (wordsSet == null) {
            throw new NullPointerException("Could not load the list");
        }
        return wordsSet;
    }
}
