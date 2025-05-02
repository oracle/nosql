/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;

public class QTOptions {

    File baseDir;

    boolean verbose = false;

    boolean progress = false;

    byte traceLevel = 0;

    String filter = null;

    String filterOut = null;

    boolean updateQueryPlans = false;

    boolean genActualFiles = false;

    boolean useAsync;

    int batchSize;

    int readKBLimit;

    boolean onprem;

    public QTOptions() {}

    public File getBaseDir() {
        return baseDir;
    }

    public void setBaseDir(File baseDir) {
        if ( !baseDir.isDirectory() ) {
            throw new IllegalArgumentException("Not a valid base " +
                "directory: " + baseDir);
        }
        this.baseDir = baseDir;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public void setProgress(boolean progress) {
        this.progress = progress;
    }

    public byte getTraceLevel() {
        return traceLevel;
    }

    public void setTraceLevel(byte l) {
        traceLevel = l;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
        if (filter != null && verbose) {
            System.out.println("  Using filter: '" + filter + "'");
        }
    }

    public String getFilterOut() {
        return filterOut;
    }

    public void setFilterOut(String filterOut) {
        this.filterOut = filterOut;
        if (filterOut != null && verbose) {
            System.out.println("  Using filterOut: '" + filterOut + "'");
        }
    }

    public void verbose(String s) {
        if (verbose) {
            System.out.println(s);
        }
    }

    public void failure(String s) {
        System.out.println(s);
    }

    public void progress(String s) {
        if (progress) {
            System.out.println(s);
        }
    }

    public String relativize(File childFile) {
        return FileUtils.relativize(baseDir, childFile);
    }

    public boolean isUpdateQueryPlans() {
        return updateQueryPlans;
    }

    public void setUpdateQueryPlans(boolean updateQueryPlans) {
        this.updateQueryPlans = updateQueryPlans;
    }

    public void setGenActualFiles(boolean genActualFiles) {
        this.genActualFiles = genActualFiles;
    }

    public boolean isGenActualFiles() {
        return genActualFiles;
    }

    public void setBatchSize(int s) {
        batchSize = s;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setReadKBLimit(int limit) {
        readKBLimit = limit;
    }

    public int getReadKBLimit() {
        return readKBLimit;
    }

    public boolean useAsync() {
        return useAsync;
    }

    public void setUseAsync(boolean v) {
        useAsync = v;
    }

    void setOnPrem(boolean v) {
        onprem = v;
    }

    public boolean getOnPrem() {
        return onprem;
    }
}
