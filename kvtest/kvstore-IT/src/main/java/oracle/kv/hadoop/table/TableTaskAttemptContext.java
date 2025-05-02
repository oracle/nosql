/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.hadoop.table;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Concrete implementation of Hadoop TaskAttemptContext interface. When
 * initializing a YARN based (MapReduce version 2) RecordReader, an
 * instance of TaskAttemptContext must be specified. This class is intended
 * to satisfy that requirement.
 */
class TableTaskAttemptContext implements TaskAttemptContext {

    private Job job;
    private String currentStatus = "UNKNOWN";

    @SuppressWarnings("deprecation")
    public TableTaskAttemptContext(Configuration jobConf) throws IOException {
        this.job = new Job(jobConf);
    }

    /*
     * Specified by TaskAttemptContext interface.
     */
    @Override
    public Counter getCounter(Enum<?> counterName) {
        return null;
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
        return null;
    }

    @Override
    public float getProgress() {
        return 0.0f;
    }

    @Override
    public String getStatus() {
        return currentStatus;
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
        return null;
    }

    @Override
    public void setStatus(String status) {
        currentStatus = status;
    }

    /**
     * Specified by <code>Progressable</code> interface
     * (<code>TaskAttemptContext</code> extends <code>Progressable</code>).
     */
    @Override
    public void progress() {
        /* No-op */
    }

    /**
     * Specified by <code>JobContext</code> interface
     * (<code>TaskAttemptContext</code> extends <code>JobContext</code>).
     *
     * Note that whereas it would be preferrable to define this class
     * to implement <code>TaskAttemptContext</code> and extend the
     * <code>Job</code> class (so that this class inherits the
     * implementations of all of the methods defined below), that cannot
     * be done. This is because the <code>Job</code> class implements
     * the method <code>JobStatus getStatus</code>, while the
     * <code>TaskAttemptContext</code> interface specifies the method
     * <code>String getStatus</code>. As a result, a sub-class of
     * <code>Job</code> cannot implement <code>TaskAttemptContext</code>;
     * because a method implementation that satisfies the
     * <code>getStatus</code> specification in <code>TaskAttemptContext</code>
     * cannot override the implementation of <code>getStatus</code>
     * provided by <code>Job</code>.
     */
    @Override
    public Path[] getArchiveClassPaths() {
        return job.getArchiveClassPaths();
    }

    @Override
    public String[] getArchiveTimestamps() {
        return job.getArchiveTimestamps();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        return job.getCacheArchives();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        return job.getCacheFiles();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
        return job.getCombinerClass();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
        return job.getCombinerKeyGroupingComparator();
    }

    @Override
    public Configuration getConfiguration() {
        return job.getConfiguration();
    }

    @Override
    public org.apache.hadoop.security.Credentials getCredentials() {
        return job.getCredentials();
    }

    @Override
    public Path[] getFileClassPaths() {
        return job.getFileClassPaths();
    }

    @Override
    public String[] getFileTimestamps() {
        return job.getFileTimestamps();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        return job.getGroupingComparator();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
        return job.getInputFormatClass();
    }

    @Override
    public String getJar() {
        return job.getJar();
    }

    @Override
    public JobID getJobID() {
        return job.getJobID();
    }

    @Override
    public String getJobName() {
        return job.getJobName();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        return job.getJobSetupCleanupNeeded();
    }

    @Deprecated
    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        return job.getLocalCacheArchives();
    }

    @Deprecated
    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        return job.getLocalCacheFiles();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        return job.getMapOutputKeyClass();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        return job.getMapOutputValueClass();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
        return job.getMapperClass();
    }

    @Override
    public int getMaxMapAttempts() {
        return job.getMaxMapAttempts();
    }

    @Override
    public int getMaxReduceAttempts() {
        return job.getMaxReduceAttempts();
    }

    @Override
    public int getNumReduceTasks() {
        return job.getNumReduceTasks();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
        return job.getOutputFormatClass();
    }

    @Override
    public Class<?> getOutputKeyClass() {
        return job.getOutputKeyClass();
    }

    @Override
    public Class<?> getOutputValueClass() {
        return job.getOutputValueClass();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
        return job.getPartitionerClass();
    }

    @Override
    public boolean getProfileEnabled() {
        return job.getProfileEnabled();
    }

    @Override
    public String getProfileParams() {
        return job.getProfileParams();
    }

    @Override
    public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
        return job.getProfileTaskRange(isMap);
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
        return job.getReducerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
        return job.getSortComparator();
    }

    @Deprecated
    @Override
    public boolean getSymlink() {
        return job.getSymlink();
    }

    @Override
    public boolean getTaskCleanupNeeded() {
        return job.getTaskCleanupNeeded();
    }

    @Override
    public String getUser() {
        return job.getUser();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        return job.getWorkingDirectory();
    }
}
