/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.nosql.proxy.util;

import static oracle.nosql.proxy.protocol.HttpConstants.X_FORWARDED_FOR_HEADER;
import static oracle.nosql.proxy.protocol.HttpConstants.X_REAL_IP_HEADER;

import java.util.concurrent.RejectedExecutionException;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.AttributeKey;
import oracle.nosql.common.cache.Cache;
import oracle.nosql.common.cache.CacheBuilder;
import oracle.nosql.common.cache.CacheBuilder.CacheConfig;
import oracle.nosql.common.ratelimit.RateLimiter;
import oracle.nosql.common.ratelimit.SimpleRateLimiter;

import java.lang.Runnable;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import oracle.kv.impl.util.RateLimiting;
import oracle.nosql.common.sklogger.ScheduleStart;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.MonitorStats;

/**
 * A class to manage errors and delaying/not responding to them.
 */
public class ErrorManager {

    /* default ip if we can't find one from the headers (cloud only) */
    private static final String DEFAULT_CLIENT_IP = "0.0.0.0";

    /* flag for bad actors */
    public static final AttributeKey<Boolean> EXCESSIVE_USE_ATTR =
        AttributeKey.valueOf("exsu");

    private final SkLogger logger;
    private final MonitorStats stats;

    /* an LRU cache of rate limiters */
    private Cache<String, RateLimiter> rateLimiters;

    /* metrics tracking raw number of delays/DNRs */
    private long delayedResponses;
    private long didNotResponds;

    /* metrics tracking number of unique IPs with delay/DNR */
    private Set<String> delayIPMap;
    private Set<String> dnrIPMap;

    /* small thread pool for managing delays / stats */
    private ScheduledThreadPoolExecutor delayPool;

    /* max number of queued items for delay pool */
    private final static int MAX_DELAY_QUEUE_SIZE = 500;

    /* error rate that triggers delaying responses (errors/sec) */
    private final int delayResponseThreshold;

    /* amount of time to delay responses in milliseconds */
    private final int delayResponseMs;

    /*
     * DNR threshold as a percentage (>=100%) of the
     * delay threshold. This is an accelerator for DNR checks in
     * incrementErrorRate().
     */
    private final double dnrPercentage;

    /* use N milliseconds of error "credit" from the past before limiting */
    private final int errorCreditMs;

    private final RateLimiting<String> logLimiter;

    /**
     * Create and start a new Error Manager.
     *
     * This implements the core error limiting logic:
     * - A RateLimiter is created for each unique IP address that causes an
     *   error (exception) that should be measured (as determined by
     *   isErrorLimitingResponse).
     * - RateLimiters are keyed by IP address
     * - the operation being executed is not considered (DML, DDL, etc)
     * - if the allowed error rate is exceeded, the response is delayed
     * - if the allowed error rate is exceeded by more, the response is dropped
     *   entirely ("Did Not Respond", or "DNR").
     *
     * @param logger Logger instance to use for messages. Messages sent to this
     *        logger are rate limited internally by this class.
     * @param stats A MonitorStats instance to use for reporting stats. The stats
     *        reported per 60 second interval are:
     *          - total delayed responses
     *          - total DNRs
     *          - unique # of IPs getting delayed responses
     *          - unique # of IPs getting DNRs
     * @param statsIntervalMs the interval at which stats are collected. Only
     *        used if above stats parameter is non-null.
     * @param config the following fields from config are used:
     *        delayResponseThreshold: Number of errors per second from any one
     *        IP that are allowed before this manager starts delaying responses.
     *
     *        delayResponseMs: Amount of time to delay responses, in millis.
     *
     *        dnrThreshold: Number of errors per second from any one IP that
     *        will trigger DNR (Do Not Respond). This must be equal or
     *        greater than delayResponseThreshold.
     *
     *        errorCreditMs: Use N milliseconds of error "credit" from the
     *        past before limiting. For example, if this value is set to 3000,
     *        and errorThreshold is 5, and no errors have happend for an IP
     *        in the last 3 seconds, allow 15 errors this second before
     *        starting to delay responses.
     *
     *        errorCacheSize: Maximum number of unique IP addresses to track
     *        error rates for.
     *
     *        errorCacheLifetimeMs: Maximum amount of time, in millis, to keep
     *        track of any specific IP error rate.
     *
     *        delaypoolSize: Size of thread pool used to manage delaying
     *        responses. This is also used once per minute to update stats.
     */
    public ErrorManager(SkLogger logger,
                        MonitorStats stats,
                        int statsIntervalMs,
                        Config config) {
        if (config.getErrorDelayResponseThreshold() <= 0) {
            throw new IllegalArgumentException(
                          "errorDelayResponseThreshold must be positive");
        }
        if (config.getErrorDnrThreshold() <
            config.getErrorDelayResponseThreshold()) {
            throw new IllegalArgumentException(
                          "errorDnrThreshold must be equal to or greater " +
                          "than delayResponseThreshold (" +
                          config.getErrorDelayResponseThreshold() + ")");
        }
        this.logger = logger;
        this.stats = stats;
        delayResponseThreshold = config.getErrorDelayResponseThreshold();
        delayResponseMs = config.getErrorDelayResponseMs();
        errorCreditMs = config.getErrorCreditMs();
        logLimiter = new RateLimiting<String>(10_000, 100);

        /*
         * DNR threshold as a percentage (>=100%) of the
         * delay threshold. This is an accelerator for DNR checks in
         * incrementErrorRate().
         */
        dnrPercentage = (double)config.getErrorDnrThreshold() * 100.0 /
                        (double)delayResponseThreshold;

        /* LRU cache */
        this.rateLimiters = CacheBuilder.build(
            new CacheConfig().setCapacity(config.getErrorCacheSize())
                             .setLifetime(config.getErrorCacheLifetimeMs()));

        this.delayPool = new ScheduledThreadPoolExecutor(
                             config.getErrorDelayPoolSize());
        if (stats != null) {
            delayIPMap = ConcurrentHashMap.newKeySet();
            dnrIPMap = ConcurrentHashMap.newKeySet();
            /* schedule stats update every 60 seconds */
            /*
             * Try to update 1 second before the start of the minute. This
             * is because the metrics are queried/sent on each minute,
             * using a ScheduledThreadPoolExecutor.scheduleAtFixedRate()
             * call very similar to this. See SkLogger/MeasureRegistry.java
             * if you're interested.
             */
            long delayMs = ScheduleStart.calculateDelay(statsIntervalMs,
                                                System.currentTimeMillis());
            delayMs -= 1000;
            if (delayMs < 0) {
                delayMs += statsIntervalMs;
            }
            delayPool.scheduleAtFixedRate(() -> {
                          stats.trackDelayedResponses(delayedResponses);
                          delayedResponses = 0;
                          stats.trackDidNotResponds(didNotResponds);
                          didNotResponds = 0;
                          stats.trackDelayedResponseIPs(delayIPMap.size());
                          delayIPMap.clear();
                          stats.trackDidNotRespondIPs(dnrIPMap.size());
                          dnrIPMap.clear();
                      }, delayMs, statsIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public void shutDown() {
        if (rateLimiters == null) {
            return;
        }
        rateLimiters.stop(false);
        delayPool.shutdownNow();
        rateLimiters = null;
    }

    /**
     * return a rate limiter for a specified key.
     */
    private RateLimiter getRateLimiter(String key, int maxRate) {
        if (rateLimiters == null) {
            return null;
        }
        if (key == null || key == "") {
            return null;
        }
        RateLimiter rl = rateLimiters.get(key);
        if (rl != null || maxRate <= 0) {
            return rl;
        }
        /* allow for a small configured burst of errors before limiting */
        rl = new SimpleRateLimiter(maxRate, (double)errorCreditMs / 1000.0);
        rl.setCurrentRate(0.0);
        rateLimiters.put(key, rl);
        return rl;
    }

    private void addUniqueIP(String clientIP, boolean DNR) {
        if (DNR) {
            dnrIPMap.add(clientIP);
        } else {
            delayIPMap.add(clientIP);
        }
    }

    private void logDNR(String clientIP,
                        FullHttpResponse response, int delayMs) {
        if (stats != null) {
            if (delayMs > 0) {
                delayedResponses++;
                addUniqueIP(clientIP, false);
            } else {
                didNotResponds++;
                addUniqueIP(clientIP, true);
            }
        }

        if (! logger.isLoggable(Level.INFO)) {
            return;
        }
        String op = ((delayMs > 0) ? "Delaying response" : "Not responding");
        String msg = op + " to IP " + clientIP +
                     " (code=" + getReturnCode(response) +
                     "): too many errors.";
        if (! logLimiter.isHandleable(msg)) {
            return;
        }
        logger.logEvent("DataService" /* category */,
                        Level.INFO,
                        op /* subject */,
                        msg /* message */,
                        null /* exception */);
    }

    public enum ErrorAction {
        RESPOND_NORMALLY, /* respond as normal (immediately) */
        RESPONSE_DELAYED, /* response will be sent later by another thread */
        DO_NOT_RESPOND    /* do not send a response */
    }

     /**
      * Increment error rate for a client.
      * @return One of the above actions:
      *         RESPOND_NORMALLY: Errors not above any threshold; continue
      *                           as normal.
      *         RESPONSE_DELAYED: Response will be sent later by a
      *                           different thread. No more action required.
      *         DO_NOT_RESPOND: Do not send a response. Clean up any
      *                         resources used by this response.
      */
    public ErrorAction incrementErrorRate(FullHttpResponse response,
                                          String clientIP,
                                          boolean excessiveUse,
                                          ChannelHandlerContext ctx,
                                          Runnable delayTask) {
        /* no need if we're not managing error limiters */
        if (rateLimiters == null) {
            return ErrorAction.RESPOND_NORMALLY;
        }
        /*
         * Look up error limiter for IPaddr. If one doesn't
         * exist already, create a new one.
         */
        if (clientIP == null || clientIP.isEmpty()) {
            return ErrorAction.RESPOND_NORMALLY;
        }
        String key = "ip:" + clientIP;
        RateLimiter rl = getRateLimiter(key, delayResponseThreshold);
        /* consume one unit without waiting */
        rl.consumeUnitsUnconditionally(1);

        /*
         * If the client is not over its error limit, and there
         * are no other indications of excessive use (bad actors, etc)
         * then return false so the response will get immediately sent.
         */
        if (rl.tryConsumeUnits(0) == true && excessiveUse == false) {
            return ErrorAction.RESPOND_NORMALLY;
        }

        /*
         * At this point the client is over its error limit, and/or
         * has been flagged for excessive use. Either delay sending the
         * response, or don't respond at all.
         */

        /*
         * If we're over the DNR limit, don't respond at all. This is
         * measured by checking current rate of the limiter, which is
         * repesented as a percentage of the delayResponseThreshold.
         * For example, if delayResponseThreshold is 5, and the limiter
         * rate is 150, that repesents a current rate of 7.5 errors/sec.
         */
        if (rl.getCurrentRate() > dnrPercentage) {
            /* do not respond */
            logDNR(clientIP, response, 0);
            return ErrorAction.DO_NOT_RESPOND;
        }

        /* if delay pool size is very large, just skip delaying */
        if (delayPool.getQueue().size() > MAX_DELAY_QUEUE_SIZE) {
            return ErrorAction.RESPOND_NORMALLY;
        }

        /* delay responding */
        try {
            delayPool.schedule(delayTask,
                               delayResponseMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            /* couldn't schedule: just do now */
            return ErrorAction.RESPOND_NORMALLY;
        }

        logDNR(clientIP, response, delayResponseMs);
        return ErrorAction.RESPONSE_DELAYED;
    }

    /**
     * Get the client IP from X-Real-IP or X-Forwarded-For headers.
     * If neither exist, return a default IP.
     */
    public static String getClientIP(FullHttpRequest request) {
        HttpHeaders headers = request.headers();
        /* with LBs, we expect this to always be set */
        String clientIP = headers.get(X_REAL_IP_HEADER);
        if (clientIP != null && clientIP != "") {
            return clientIP;
        }
        String xff = headers.get(X_FORWARDED_FOR_HEADER);
        if (xff != null && xff != "") {
            /* the first addr in X-F-F is the client */
            int offset = xff.indexOf(",");
            if (offset < 0) {
                clientIP = xff;
            } else {
                clientIP = xff.substring(0, offset);
            }
        } else {
            /* TODO: use ctx.channel().remoteAddress().toString() ? */
            /* that will likely just be the LB ipaddr */

            /* TODO: log this */
            clientIP = DEFAULT_CLIENT_IP; // across all clients
        }
        return clientIP;
    }

    /* this is only used for logging. As such, it is best-effort. */
    private static int getReturnCode(FullHttpResponse resp) {
        if (resp == null) {
            return -1;
        }
        if (resp.status().code() != 200) {
            return resp.status().code();
        }
        if (resp.content() != null && resp.content().capacity() > 0) {
            /* peek at the first byte of response to determine response code */
            return resp.content().getByte(0);
        }
        return -1;
    }
}
