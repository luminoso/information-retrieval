package pt.ua.deti.ir.Utils.Mem;

/**
 * Memory utility class that allows memory conversions and JVM memory counter getters
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class MemUtil {


	/**
	 * Convert Bytes to KBytes
	 * @param bytes
	 * @return
	 */
    private static long toKB(long bytes) {
        return bytes / MemSize.KB.toLong();
    }

    /**
     * Convert Bytes to MBytes
     * @param bytes
     * @return
     */
    public static long toMB(long bytes) {
        return bytes / MemSize.MB.toLong();
    }

    /**
     * Convert Bytes to GBytes
     * @param bytes
     * @return
     */
    private static long toGB(long bytes) {
        return bytes / MemSize.GB.toLong();
    }

    /**
     * Getter - Runtime free memory
     * @param rt
     * @return
     */
    public static long getFreeMem(Runtime rt) {
        switch (MemSize.MB) {
            case KB:
                return toKB(rt.freeMemory());
            case MB:
                return toMB(rt.freeMemory());
            case GB:
                return toGB(rt.freeMemory());
            default:
                return -1L;
        }
    }

    /**
     * Getter - Runtime total memory
     * @param rt
     * @return
     */
    public static long getTotalMem(Runtime rt) {
        switch (MemSize.MB) {
            case KB:
                return toKB(rt.totalMemory());
            case MB:
                return toMB(rt.totalMemory());
            case GB:
                return toGB(rt.totalMemory());
            default:
                return -1L;
        }
    }

    /**
     * Getter - Runtime max memory
     * @param rt
     * @return
     */
    public static long getMaxMem(Runtime rt) {
        switch (MemSize.MB) {
            case KB:
                return toKB(rt.maxMemory());
            case MB:
                return toMB(rt.maxMemory());
            case GB:
                return toGB(rt.maxMemory());
            default:
                return -1L;
        }
    }
}
