package oracle.nosql.util;

/**
 * Utility to manage the time period allocated for a command to execute.
 * It lets you specify a total wait period and retry intervals, and tracks
 * the time and sleeps.
 */
public class RetryHelper {

    /* This retry interval determines how long you sleep between retries. */
    public static final long DEFAULT_RETRY_INTERVAL = 60_000;

    /* All times are in milliseconds */
    private final long currentTime;
    private final long totalWait;
    private final long retryInterval;
    private final long endTime;

    /**
     * Instantiating the helper sets the current time and end time for the wait.
     */
    public RetryHelper(long totalWait, long retryInterval) {
        this.currentTime = System.currentTimeMillis();
        this.endTime = currentTime + totalWait;
        this.retryInterval = retryInterval;
        this.totalWait = totalWait;
    }

    public boolean isDone() {
        return System.currentTimeMillis() > endTime;
    }

    public void sleep() throws InterruptedException {
        if (retryInterval != 0) {
            Thread.sleep(retryInterval);
        }
    }

    /*
     * Sleep and ignore any interrupts. Use this carefully, perhaps only
     * for tests, as we need to consider how to handle interrupts!
     */
    public void sleepIgnoreInterrupt() {
        try {
            sleep();
        } catch (InterruptedException ignore) {
        }
    }

    public String displayWait() {
        return totalWait + " milliseconds";
    }
}
