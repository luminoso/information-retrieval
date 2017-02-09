package pt.ua.deti.ir.Utils.Disk;

import org.nustaq.serialization.FSTObjectOutput;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The DiskManager provides a way to abstract disk operations on the file system.
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class DiskManager {

    public final String dirname;
    //diskmgr bits
    private final ConcurrentSkipListSet<File> rfiles = new ConcurrentSkipListSet<>();
    private final File dir;

    /**
     * Initializes a DiskManager bound on a certain directory
     * @param dirname
     */
    public DiskManager(String dirname) {
        dir = new File(dirname);
        this.dirname = dir.getPath();

        if (!dir.exists()) {
            dir.mkdirs();
        }

        buildDir();
    }

    /**
     * Write an Object into a file, using a serialized format
     *
     * @param fileName
     * @param object
     */
    public synchronized void write(String fileName, Object object) {
        FileOutputStream fout;
        FSTObjectOutput oos;

        try {
            fout = new FileOutputStream(dirname + "/" + fileName);
            oos = new FSTObjectOutput(fout);
            oos.writeObject(object);
            oos.close();
            fout.close();
        } catch (IOException e) {
            Logger.getLogger(DiskManager.class.getName()).log(Level.SEVERE, null, e);
        }

        //update the directory files list
        rfiles.add(new File(dirname + "/" + fileName));
    }

    /**
     * Read and unserialize method for ConcurrentTermMap objects
     *
     * @param fileName
     * @return
     */
    public Object read(String fileName) {
        return DiskUtils.read(fileName);
    }

    /**
     * Scan the defined directory and populate the files ArrayList
     */
    private void buildDir() {
        Collections.addAll(rfiles, dir.listFiles());
    }

    /**
     * Given a certain key, it returns how many files in the directory contain
     * that key in the dot format used. Key = file1 Directory: file1.0 file1.1
     * It would return 2.
     *
     * @param key
     * @return
     */
    public long dirCountContains(String key) {
        buildDir();

        return rfiles.parallelStream()
                .filter(e -> e.getName().split("\\.")[0].equals(key))
                .distinct()
                .count();
    }

    /**
     * Get a list of the directory
     * @return
     */
    public ConcurrentSkipListSet<File> getRFiles() {
        //update the directory before returning it
        buildDir();
        return rfiles;
    }

    /**
     * Remove a certain file from the list
     * @param name
     */
    public void rfilesRemove(String name) {
        rfiles.parallelStream().forEach(file ->
        {
            if (file.getName().equals(name)) {
                rfiles.remove(file);
            }
        });
    }
}
