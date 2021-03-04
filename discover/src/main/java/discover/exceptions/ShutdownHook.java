package discover.exceptions;

import discover.components.ExecutionPreProcessor;

/**
 * This class is used to clean up before exiting,
 * if any termination signal hits the process.
 */
public class ShutdownHook extends Thread {

    /**
     * This function runs the actual clean up code.
     */
    public void run() {
        System.out.println("[INFO] Shutdown hook called. Dropping Tables....");
        synchronized(ExecutionPreProcessor.class) {
            ExecutionPreProcessor.dropAllTempTables();
        }
        System.out.println("[INFO] Tables Dropped");
    }
}