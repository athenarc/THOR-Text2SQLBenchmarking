package spark.testing;

import java.lang.management.ManagementFactory;

public class PerformanceMonitor {
    
    long cpuStartTime;
    long elapsedTime;
    int cpuCount;
    long beforeUsedMem;

    public static PerformanceMonitor builder() {
        return new PerformanceMonitor();
    }

    /**
     * Start monitoring cpu
     * 
     * @return Itself
     */
    public PerformanceMonitor startCpuMonitor() {            
        this.cpuCount = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();        
        this.cpuStartTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime();
        this.elapsedTime = System.nanoTime();
        return this;
    }


    /**
     * Monitor CPU usage
     * 
     * @return
     */
    public PerformanceMonitor startMemMonitor() {
        this.beforeUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        return this;
    }

    /**
     * Calculate cpu % used*
     * 
     * @return
     */
    public int calcCpuPer() {
        long end = System.nanoTime();
        long totalAvailCPUTime = this.cpuCount * (end-this.elapsedTime);
        long totalUsedCPUTime = ManagementFactory.getThreadMXBean().getCurrentThreadCpuTime()-this.cpuStartTime;        
        float per = ((float)totalUsedCPUTime*100)/(float)totalAvailCPUTime;        
        return (int)per;
    }


    /**
     * Calculate before memory usage in mega bytes
     * 
     * @return
     */
    public double calcMemUsage() {
        long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        return (afterUsedMem-this.beforeUsedMem) / (1024.0 * 1024.0);
    }

    
}
