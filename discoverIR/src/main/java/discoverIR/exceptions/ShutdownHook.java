package discoverIR.exceptions;

import discoverIR.components.execution.ExecutionPreProcessor;;

/**
 * This class is used to clean up before exiting,
 * if any termination signal hits the process.
 */
public class ShutdownHook extends Thread {

    ExecutionPreProcessor executionPreProcessor; // The pre processor is the only Object that requires clean up.

    /** Constructor */
    public ShutdownHook(ExecutionPreProcessor executionPreProcessor){
        this.executionPreProcessor = executionPreProcessor;
    }

    /**
     * This function runs the actual clean up code.
     */
    public void run() {
        System.out.println("[INFO] Shutdown hook called. Dropping Tables....");
        synchronized(this.executionPreProcessor) {
            this.executionPreProcessor.dropAllTempTables();
        }
        System.out.println("[INFO] Tables Dropped");
    }
}