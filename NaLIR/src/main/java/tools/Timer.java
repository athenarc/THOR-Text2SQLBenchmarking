package tools;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

// This class implements a timer to measure the time taken by the components of the system.
public class Timer {

    // Used to decide whether to measure CPU or wall clock time.
    public enum Type {
        CPU_TIME,
        WALL_CLOCK_TIME
    }

    Type type;
    private long startTime;

    public Timer() {
        this.type = Type.CPU_TIME; // The default measurement is CPU time.
    }

    public Timer(Type type) {
        this.type = type;
    }

    // Getters and Setters.
    public Type getType() {
        return this.type;
    }

    // Starts the timer.
    public void start() {
        switch (this.type) {
            case CPU_TIME:
                this.startTime = getCpuTime(); // CPU time is measured in nanoseconds.
                break;
            case WALL_CLOCK_TIME:
                this.startTime = getWallClockTime(); // Wall clock time is measured in milliseconds.
                break;
        }
    }

    // Stops the timer and returns the elapsed time in seconds.
    public Double stop() {
        switch (this.type) {
            case CPU_TIME:
                return (double) (getCpuTime() - this.startTime) / 1_000_000_000.0;
            case WALL_CLOCK_TIME:
                return (double) (getWallClockTime() - this.startTime) / 1_000.0;
            default:
                return 0.0;
        }
    }

    // Returns the cpu time that a thread has been active for in nanoseconds.
    private long getCpuTime() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        return bean.isCurrentThreadCpuTimeSupported() ? bean.getCurrentThreadCpuTime() : 0L;
    }

    // Returns the current time in milliseconds.
    private long getWallClockTime() {
        return System.currentTimeMillis();
    }

    @Override
    public String toString() {        
        return this.stop() + " (s)";
    }

}