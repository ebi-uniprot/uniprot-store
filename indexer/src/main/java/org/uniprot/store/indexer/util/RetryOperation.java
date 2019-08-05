package org.uniprot.store.indexer.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given an operation encapsulated within a {@link RetryOperation.Operation}
 * function, retries the execution of the function.
 * <p>
 * If the run is successful the class returns the function result, if an exception is thrown during execution, the
 * class retries until it is successful or the maximum number of run attempts is reached.
 *
 * @param <T> The expected operation result
 */
public class RetryOperation<T> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final int maxAttempts;

    private int secondToWait;
    private int attemptCounter;

    /**
     * Creates an instance with a predefined maximum number of run attempts, and the ammount of seconds to wait
     * between retries
     *
     * @param maxAttempts   number of run attempts
     * @param secondsToWait the number of seconds to wait before retying the operation execution
     */
    public RetryOperation(int maxAttempts, int secondsToWait) {
        this(maxAttempts);
        Preconditions.checkArgument(secondsToWait >= 0, "The number of seconds to wait between retries should be " +
                "greater or equal to 0 ");
        this.secondToWait = secondsToWait * 1000;
    }

    /**
     * Creates an instance with a predefined maximum number of run attempts
     *
     * @param maxAttempts number of run attempts
     * @throws IllegalArgumentException if macAttempts is less than one
     */
    public RetryOperation(int maxAttempts) {
        Preconditions.checkArgument(maxAttempts > 0, "Number of retry attempts should be greater than 1");
        this.maxAttempts = maxAttempts;
        this.attemptCounter = 0;
    }

    /**
     * Accepts a {@link RetryOperation.Operation} function with the operation to
     * execute within it. The method will run the operation until it returns a result, or after several several failed
     * attempts will throw an exception.
     *
     * @param operation the operation to execute
     * @return the result of the operation execution
     * @throws RetryException is thrown after retrying several failed attempts to execute the operation
     */
    public T run(Operation<T> operation) {
        try {
            return operation.execute();
        } catch (Exception e) {
            return retry(operation);
        }
    }

    private T retry(Operation<T> operation) {
        while (attemptCounter < maxAttempts) {
            attemptCounter++;
            try {
                logger.info("Retrying attempt #{}:", attemptCounter);
                return operation.execute();
            } catch (Exception e) {
                logger.warn("Retry attempt #{} failed:", attemptCounter, e);
                try {
                    Thread.sleep(secondToWait);
                } catch (InterruptedException e1) {
                    logger.trace("Error occurred whilst sleeping", e1);
                }
            }
        }

        throw new RetryException("Max number of retries: " + maxAttempts + ", reached without success. " +
                                         "Giving up on operation.");
    }

    int getNumberOfRetries() {
        return attemptCounter;
    }

    public interface Operation<T> {
        T execute() throws Exception;
    }
}