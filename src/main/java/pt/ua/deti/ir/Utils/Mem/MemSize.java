package pt.ua.deti.ir.Utils.Mem;

/**
 * MemSize enumerator provides the memory sizes (KB, MB and GB) as objects
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public enum MemSize {
    KB(1024L),
    MB(MemSize.KB.toLong() * MemSize.KB.toLong()),
    GB(MemSize.KB.toLong() * MemSize.MB.toLong());

    private final long size;

    /**
     * Initialize an enum 
     * @param size
     */
    MemSize(long size) {
        this.size = size;
    }

    /**
     * Getter - size as a long
     * @return
     */
    public long toLong() {
        return this.size;
    }
}
