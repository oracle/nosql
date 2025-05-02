/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.contextlogger;

import java.io.Serializable;
import java.util.logging.Level;

import oracle.nosql.common.json.JsonUtils;

/**
 * A LogContext establishes a logging context for a SkLogger.log call.
 * The logging context contains information about the original API request
 * that resulted in the execution of the code that produced a log record.
 * This allows the log records that are associated with the same request to
 * be grouped together during postprocessing of the log.
 *
 * The LogContext also establishes the current environmental log level for the
 * logging call, overriding the logger's current log level.
 *
 */
public class LogContext implements Serializable {

    private static final long serialVersionUID = 1;

    protected String id;      /* Keep as a String to avoid conversions. */
    protected String entry;   /* API entrypoint that created this context. */
    protected String origin;  /* User that invoked the entrypoint. */
    protected int logLevel;   /* Environmental log level to apply. */
    protected boolean logged; /* Indicates whether this context was emitted. */

    /* No-arg constructor for Json serialization. */
    public LogContext() {
    }

    public LogContext(CorrelationId id, /* Must be provided. */
                      String entry,     /* Can be null. */
                      String origin,    /* Can be null. */
                      Level logLevel) { /* Can be null. */
        this(id, entry, origin, logLevel, false);
    }

    protected LogContext(CorrelationId id,
                       String entry,
                       String origin,
                       Level logLevel,
                       boolean logged) {
        if (id == null) {
            throw new IllegalStateException("CorrelationId cannot be null.");
        }
        this.id = id.toString();
        this.entry = entry;
        this.origin = origin;
        this.logLevel = logLevel.intValue();
        this.logged = logged;
    }

    public LogContext(String id,
                      String entry,
                      String origin,
                      int logLevel,
                      boolean logged) {
        this.id = id;
        this.entry = entry;
        this.origin = origin;
        this.logLevel = logLevel;
        this.logged = logged;
    }

    public void setId(String id) {
        this.id = id;
        resetLogged();
    }

    public String getId() {
        return id;
    }

    public String getEntry() {
        return entry;
    }

    public String getOrigin() {
        return origin;
    }

    public void putOrigin(String newOrigin) {
        origin = newOrigin;
        resetLogged();
    }

    public int getLogLevel() {
        return logLevel;
    }

    public void putLogLevel(Level newLevel) {
        logLevel = newLevel.intValue();
        resetLogged();
    }

    public boolean isLogged() {
        return logged;
    }

    public void setLogged() {
        logged = true;
    }

    private void resetLogged() {
        logged = false;
    }

    public String toJsonString() {
        return JsonUtils.toJson(this);
    }

    @Override
    public String toString() {
        return toJsonString();
    }

    static public LogContext fromJsonString(String js) {
        if (js == null) {
            return null;
        }
        return JsonUtils.fromJson(js, LogContext.class);
    }

    @Override
    /* boolean logged does not participate. */
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((entry == null) ? 0 : entry.hashCode());
        result = prime * result + ((origin == null) ? 0 : origin.hashCode());
        result = prime * result + logLevel;
        return result;
    }

    @Override
    /* boolean logged does not participate. */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LogContext other = (LogContext) obj;
        if (entry == null) {
            if (other.entry != null) {
                return false;
            }
        } else if (!entry.equals(other.entry)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (logLevel != other.logLevel) {
            return false;
        }
        if (origin == null) {
            if (other.origin != null) {
                return false;
            }
        } else if (!origin.equals(other.origin)) {
            return false;
        }
        return true;
    }
}
