/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.audit.oci;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.Stack;

import oracle.nosql.audit.AuditContext;
import oracle.nosql.audit.Auditor;
import oracle.nosql.common.JsonBuilder;

/**
 * Log the AuditSchema for OCI audit.
 * It writes logs to /data/audit2/muxed.audit (with rotation to
 * /data/audit2/muxed-%d{yyyy-MM-dd_HH}.audit) to leverage Chainsaw for sending
 * data to the Audit Service back-end.
 * Each log entry must be in a single line delimited by a line break.
 *
 * The audit log is format as:
 * {"compartment":<compartmentID>, "content":"<escaped OCIAuditContext>"}
 * Note:
 * The content OCIAuditContext is from OCIAuditContext objects serialized as a
 * JSON string. And the serialized JSON string must be string-escaped.
 * Example:
 * {"compartment":"xxxx","content":"{\"cloudEventsVersion\":\"0.1\",..."}
 * Details:
 * https://confluence.oci.oraclecorp.com/pages/viewpage.action?pageId=109221866
 */
public class OCIAuditor implements Auditor {

    public final static String AUDIT_DIR = "/data/audit2";
    public final static int HISTORY_MAX_DEFAULT = 100;
    public final static TemporalUnit PERIOD_DEFAULT = ChronoUnit.DAYS;

    private final static String AUDIT_FILENAME = "muxed.audit";
    private final static String AUDIT_ROTATE_PREFIX = "muxed-";
    private final static String AUDIT_ROTATE_SUFFIX = ".audit";

    private final DateFormat formatter =
        new SimpleDateFormat("yyyy-MM-dd_HH");
    private final Date formatDate = new Date();
    private final File auditDir;
    private final File auditFile;
    /*
     * When will rotate audit log, like per day, per hour.
     */
    private final TemporalUnit rotatePeriod;
    /*
     * Record all audit files, order by time asc.
     */
    private final Stack<File> historyAuditFiles;
    /*
     * When audit files reach historyMax, oldest audit log will be deleted.
     */
    private final int historyMax;

    private PrintWriter auditWriter;
    private long nextRotateTime;

    public OCIAuditor() throws IOException {
        this(AUDIT_DIR, PERIOD_DEFAULT, HISTORY_MAX_DEFAULT);
    }

    public OCIAuditor(String auditDir,
                      TemporalUnit rotatePeriod,
                      int historyMax) throws IOException {
        this.auditDir = new File(auditDir);
        this.auditFile = new File(auditDir, AUDIT_FILENAME);
        this.historyMax = historyMax;
        this.rotatePeriod = rotatePeriod;
        this.nextRotateTime = -1;
        historyAuditFiles = getHistoryAuditFiles();
        openAuditWriter(true /* append */);
    }

    @Override
    public synchronized void audit(AuditContext context) throws IOException {
        String log = convertAuditContextToString(context);
        if (log == null) {
            return;
        }
        /*
         * Check if audit file need be rotated before logging.
         */
        long now = System.currentTimeMillis();
        if (nextRotateTime < 0) {
            /*
             * Audit first start.
             */
            nextRotateTime = getNextRotateTime(now);
        }
        if (now > nextRotateTime) {
            /*
             * Reach the rotated time.
             */
            final String rotateName = getRotatedName(nextRotateTime);
            nextRotateTime = getNextRotateTime(now);
            final File rotateFile = new File(auditDir, rotateName);
            auditFile.renameTo(rotateFile);
            historyAuditFiles.push(rotateFile);
            while (historyAuditFiles.size() > historyMax) {
                File expiredFile = historyAuditFiles.pop();
                expiredFile.delete();
            }
            openAuditWriter(false);
        }

        auditWriter.println(log);
    }

    /*
     * Convert AuditContext to string to be audited.
     */
    protected String convertAuditContextToString(AuditContext context) {
        if (!(context instanceof OCIAuditContext)) {
            return null;
        }
        final OCIAuditContext ociContext = (OCIAuditContext) context;
        final String partitionId = ociContext.getPartitionId();
        final String content = ociContext.toJsonString();
        final AuditSchema schema =  new AuditSchema(partitionId, content);
        return schema.toJsonString();
    }

    /*
     * Find all audit files in audit dir, order by time asc.
     */
    Stack<File> getHistoryAuditFiles() throws IOException {
        if (!auditDir.exists()) {
            auditDir.mkdirs();
        }
        if (!auditDir.isDirectory() || !Files.isWritable(auditDir.toPath())) {
            throw new IOException("Unable to audit at " + auditDir);
        }
        final Stack<File> files = new Stack<>();
        Files.list(auditDir.toPath()).
              filter(path -> path.toString().endsWith(AUDIT_ROTATE_SUFFIX) &&
                             !path.endsWith(AUDIT_FILENAME)).
              sorted().
              forEach(path -> {
                  files.push(path.toFile());
               });
        return files;
    }

    private void openAuditWriter(boolean append) throws IOException {
        if (nextRotateTime < 0 && auditFile.exists()) {
            /*
             * When auditor is restarted.
             */
            nextRotateTime = getNextRotateTime(auditFile.lastModified());
        }
        /*
         * AutoFlush can be adjusted if Auditor has performance impact.
         * It is fine now as we only audit DDL operations.
         */
        auditWriter = new PrintWriter(new BufferedWriter(
                                          new FileWriter(auditFile, append)),
                                      true /* autoFlush */);
    }

    /*
     * Get next rotate time of the specified time.
     */
    long getNextRotateTime(long lastModified) {
        return Instant.ofEpochMilli(lastModified).
                       atOffset(ZoneOffset.UTC).
                       plus(1, rotatePeriod).
                       truncatedTo(rotatePeriod).
                       toInstant().toEpochMilli();
    }

    String getRotatedName(long rotateTime) {
        formatDate.setTime(rotateTime);
        return AUDIT_ROTATE_PREFIX +
               formatter.format(formatDate) +
               AUDIT_ROTATE_SUFFIX;
    }

    /**
     * This is OCI required schema that will be audited.
     */
    public static class AuditSchema {
        private static final String COMPARTMENT_FIELD = "compartment";
        private String compartment;

        private static final String CONTENT_FIELD = "content";
        private String content;

        public AuditSchema() {
        }

        /**
         * @param compartment is the compartmentId or tenantId if compartmentId
         * is null.
         * @param content is from AuditContext.
         */
        public AuditSchema(String compartment, String content) {
            this.compartment = compartment;
            this.content = content;
        }

        public AuditSchema(OCIAuditContext context) {
            this.compartment = context.getPartitionId();
            this.content = context.toJsonString();
        }

        public String getCompartment() {
            return this.compartment;
        }

        public String getContent() {
            return this.content;
        }

        public String toJsonString() {
            final JsonBuilder jb = JsonBuilder.create();
            jb.append(COMPARTMENT_FIELD, compartment);
            jb.append(CONTENT_FIELD, content);
            return jb.toString();
        }
    }
}
