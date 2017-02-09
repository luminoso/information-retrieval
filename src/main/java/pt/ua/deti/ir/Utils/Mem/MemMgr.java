package pt.ua.deti.ir.Utils.Mem;

import pt.ua.deti.ir.Log;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * MemMgr (please read memory manager) manages the JVM current memory status
 * @author Guilherme Cardoso gjc@ua.pt
 * @author Rui Pedro ruifpedro@ua.pt
 */
public class MemMgr implements Runnable {

    private final static Logger LOGGER = Logger.getLogger(Log.class.getName());
    private final long maxMem;
    private Runtime rt;
    private AtomicLong freeMem;
    private int pollingTime;
    private boolean stopFlag;
    private long peakMem;
    private long usedMem;
    private double usedPer;

    /**
     * Initializes a MemMgr
     */
    public MemMgr() {
        this.setRt(Runtime.getRuntime());
        setFreeMem(new AtomicLong(MemUtil.getFreeMem(getRt())));
        maxMem = MemUtil.getMaxMem(getRt());
        //TODO - extract configuration
        setPollingTime(5);
        setStopFlag(false);
        setPeakMem(0);
        setUsedMem(0);
        setUsedPer(0.0);
    }


    @Override
    public void run() {
        while (!isStopFlag()) {
            try {
                Thread.sleep(getPollingTime());
                updateMem();
            } catch (InterruptedException e) {
                setStopFlag(true);
                LOGGER.finest("Stopping memory monitor...");
            }
        }
    }

    /**
     * Update the memory counters
     */
    private void updateMem() {
        updateFreeMem();
        updateUsedMem();
        updatePeakMem();
    }

    /**
     * Update the used memory 
     */
    private void updateUsedMem() {
        setUsedMem(MemUtil.getTotalMem(getRt()) - MemUtil.getFreeMem(getRt()));
        setUsedPer(getUsedMem() / (double) getMaxMem());
    }

    /**
     * Update the peak memory
     */
    private void updatePeakMem() {
        if (peakMem < usedMem) {
            setPeakMem(usedMem);
        }
    }

    /**
     * Update the free memory
     */
    private void updateFreeMem() {
        freeMem.set(MemUtil.getFreeMem(this.getRt()));
    }

    /**
     * Set the free memory
     */
    private void setFreeMem(AtomicLong freeMem) {
        this.freeMem = freeMem;
    }

    /**
     * Getter - max memory
     */
    public long getMaxMem() {
        return maxMem;
    }

    /**
     * Getter - stoppage flag
     * @return
     */
    private boolean isStopFlag() {
        return stopFlag;
    }

    /**
     * Setter - stoppage flag
     * @param stopFlag
     */
    public final void setStopFlag(boolean stopFlag) {
        this.stopFlag = stopFlag;
    }

    /**
     * Getter - MemMgr polling time
     * @return
     */
    private int getPollingTime() {
        return pollingTime;
    }

    /**
     * Setter - MemMgr polling time
     * @param pollingTime
     */
    private void setPollingTime(int pollingTime) {
        this.pollingTime = pollingTime;
    }

    /**
     * Getter - JVM Runtime
     * @return
     */
    private Runtime getRt() {
        return rt;
    }

    /**
     * Setter - JVM Runtime
     * @param rt
     */
    private void setRt(Runtime rt) {
        this.rt = rt;
    }

    /**
     * Setter - Peak memory
     * @param peakMem the peakMem to set
     */
    private void setPeakMem(long peakMem) {
        this.peakMem = peakMem;
    }

    /**
     * Getter - Used memory
     * @return the usedMem
     */
    public long getUsedMem() {
        return usedMem;
    }

    /**
     * Setter - Used memory
     * @param usedMem the usedMem to set
     */
    private void setUsedMem(long usedMem) {
        this.usedMem = usedMem;
    }

    /**
     * Getter - Used memory percentage
     * @return
     */
    public double getUsedPer() {
        return usedPer;
    }

    /**
     * Setter - Used memory percentage
     * @param usedPer
     */
    private void setUsedPer(double usedPer) {
        this.usedPer = usedPer;
    }
}
