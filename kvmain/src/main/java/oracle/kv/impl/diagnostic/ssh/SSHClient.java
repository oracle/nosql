/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.diagnostic.ssh;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.nio.channels.UnresolvedAddressException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import oracle.kv.impl.admin.param.BootstrapParams;
import oracle.kv.impl.diagnostic.JavaVersionVerifier;
import oracle.kv.impl.diagnostic.SNAInfo;
import oracle.kv.impl.security.util.ShellPasswordReader;
import oracle.kv.impl.util.ConfigUtils;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ChannelExec;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.config.keys.ClientIdentity;
import org.apache.sshd.client.future.AuthFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.SshException;
import org.apache.sshd.sftp.client.SftpClient;
import org.apache.sshd.sftp.client.SftpClient.Attributes;
import org.apache.sshd.sftp.client.SftpClient.DirEntry;
import org.apache.sshd.sftp.client.SftpClientFactory;

/**
 * A SSH client is to connect with SSH server and it executes all remote
 * commands in SSH server to exchange info, generate files and get files.
 */
public class SSHClient {
    /* SSH client open and authentication verify wait time */
    private static final int VERIFY_WAIT_SECONDS = 300;

    /* Size of buffer used in decompress file */
    private static final int ZIP_BUFFER_SIZE = 4 * 1024;

    /* SSH port */
    private static final int SSH_PORT = 22;

    /* Files to ignore, using shell file pattern syntax */
    private static String[] FILTERS = new String[] {"*.bup",
                                                    "*.jdb",
                                                    "*/admin*/webapp/*"};
    private static final String LINUX_HOME_SIGN = "~";
    private static String USER_HOME = System.getProperty("user.home");

    /* Disable use of bouncy castle */
    static {
        System.setProperty("org.apache.sshd.security.provider.BC.enabled",
                           "false");
    }

    /* command execution begin and end time */
    private long begin;
    private long end;

    private String host = null;
    private String username = null;
    private boolean openStatus = false;
    private SshClient client = null;
    private ClientSession session = null;
    private Set<String> errorMessage = new HashSet<>();

    public SSHClient(String host, String username) {
        this.client = createClient();
        this.host = host;
        this.username = username;
        client.start();
    }

    /*
     * Create ssh client. Avoid error output produced by slf4j when
     * there is no slf4j api implementation available.
     */
    private SshClient createClient() {
        ByteArrayOutputStream errBytes = new ByteArrayOutputStream();
        PrintStream originalErr = System.err;
        System.setErr(new PrintStream(errBytes));
        SshClient newClient = null;
        try {
            newClient = SshClient.setUpDefaultClient();

        } finally {
            System.setErr(originalErr);
        }
        String errOutput = errBytes.toString();
        if (errOutput != null &&
            !errOutput.contains("Defaulting to no-operation (NOP) logger")) {
            System.err.println(errOutput);
        }
        return newClient;
    }

    /**
     * Open client by available authentication file in <core>~/.ssh</code>
     * directory. Only non-encrypted private key file is supported.
     *
     * After open, call {@link #isOpen()} to check if success. If not, call
     * {@link #getErrorMessage()} to get errors.
     */
    public void openByAuthenticatedFile() {
        createSessionByAuthFile();
        setStatus();
    }

    /**
     * Open client by password. This method will ask user to input SSH password.
     *
     * After open, call {@link #isOpen()} to check if success. If not, call
     * {@link #getErrorMessage()} to get errors.
     */
    public void openByPassword() {
        createSessionByPassword();
        setStatus();
    }

    /**
     * Check whether client is open
     *
     * @return true when client is open, or false
     */
    public boolean isOpen() {
        return openStatus;
    }

    /**
     * Get error message indicating the reason causing client is not open
     * correctly
     *
     * @return error message
     */
    public String getErrorMessage() {
        if (openStatus) {
            return null;
        }
        return errorMessage.toString();
    }

    /**
     * Close existing SSH session and stop the client.
     */
    public void close() {
        openStatus = false;
        errorMessage.clear();
        if (session != null && !session.isClosed()) {
            try {
                session.close();
            } catch (IOException e) {
                /* ignore close exception */
            }
        }
        if (client != null) {
            client.stop();
        }
    }

    /*
     * Open SSH client for unit test. It tries to open SSH client and establish
     * session with authentication file first, then with password.
     */
    void open() {
        /* Create session by public authenticated file */
        createSessionByAuthFile();
        if (session == null || session.isClosed()) {
            /* create session by password */
            createSessionByPassword();
        }

        setStatus();
    }

    /*
     * Open client for unit test
     */
    void open(String password) {
        /* Create session by public authenticated file */
        createSessionByAuthFile();
        if (session == null || session.isClosed()) {
            /* create session by password */
            createSessionByPassword(password);
        }

        setStatus();
    }

    /* Open client by password for unit test */
    void openByPassword(String password) {
        createSessionByPassword(password);
        setStatus();
    }

    /* Set status of client */
    private void setStatus() {
        if (session != null && !session.isClosed()) {
            errorMessage.clear();
            openStatus = true;
        } else {
            openStatus = false;
        }
    }

    /*
     * Create SSH client session with authentication files in
     * the ~/.ssh directory
     */
    private void createSessionByAuthFile() {
        File authKeyDirectory = new File(USER_HOME + "/.ssh");
        if (!authKeyDirectory.exists() || authKeyDirectory.isFile()) {
            /* no authentication file available, return */
            return;
        }

        try {
            Map<String, KeyPair> identities = ClientIdentity.
                loadDefaultIdentities(authKeyDirectory.toPath(),
                                      false /* strict */,
                                      null /* no encrypted private key */,
                                      LinkOption.NOFOLLOW_LINKS);

            /* Attempt to authenticate with each file till succeed */
            for (Map.Entry<String, KeyPair> entry : identities.entrySet()) {
                createClientSession(null /* pwd */, entry.getValue());
                if (session != null && session.isOpen()) {
                    return;
                }
            }
        } catch (IOException | GeneralSecurityException e) {
            errorMessage.add("Error load ssh keys, " + e.getMessage());
        }
    }

    /* Create SSH session using prompt password */
    private void createSessionByPassword() {
        createSessionByPassword(null);
    }

    /*
     * Create SSH session using password. If no password specified, this method
     * will ask user to input password.
     */
    private void createSessionByPassword(String password) {
        if (password == null) {
            try {
                password = promptPassword();
            } catch (IOException e) {
                errorMessage.add(
                    "Getting user password error, error: " + e.getMessage());
                return;
            }
        }
        if (password == null) {
            errorMessage.add("Unable to create client session with password, " +
                             "no password specified");
            return;
        }
        createClientSession(password, null);
    }

    private void createClientSession(String password, KeyPair identity) {
        ClientSession csess = null;
        try {
            csess = client.connect(username, host, SSH_PORT).
                verify(VERIFY_WAIT_SECONDS, TimeUnit.SECONDS).getSession();
            if (password != null) {
                csess.addPasswordIdentity(password);
            }
            if (identity != null) {
                csess.addPublicKeyIdentity(identity);
            }
            AuthFuture authFuture = csess.auth().verify(
                VERIFY_WAIT_SECONDS, TimeUnit.SECONDS);
            if (authFuture.isFailure()) {
                Throwable error = authFuture.getException();
                errorMessage.add(
                    "Authentication failed for " +
                    username + " to connect " + host +
                    ((error != null) ? ", error: " + error.getMessage() : ""));
            }
            session = csess;

            /* avoid finally clause */
            csess = null;
        } catch (IOException ioe) {
            StringBuilder error = new StringBuilder();
            error.append("Failed to ssh ").append(host);
            if (ioe instanceof SshException) {
                SshException se = (SshException) ioe;
                if (se.getCause() instanceof UnresolvedAddressException) {
                    error.append(", could not resolve hostname");
                } else if (se.getCause() instanceof ConnectException) {
                    error.append(", unable to connect");
                } else if (se.getMessage() != null) {
                    error.append(", ").append(se.getMessage());
                }
            } else {
                error.append(", ").append(ioe.getMessage());
            }
            errorMessage.add(error.toString());
        } finally {
            if (csess != null) {
                try {
                    csess.close();
                } catch (IOException e) {
                    /* ignore */
                }
            }
        }
    }

    /**
     * Check whether a file exists in remote machine via Linux ls command.
     *
     * @param filePath file path in remote machine
     * @return true when the file exist; or return false
     * @throws IOException if ls remote execution fail unexpectedly.
     */
    public boolean checkFile(String filePath)
        throws IOException {

        /* Generate command of checking */
        String command = "ls " + filePath;

        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();

        /*
         * The file does exist in remote machine, when the status of execution
         * of "ls <filePath>" is true.
         */
        return executeCommand(command, out, err);
    }

    /* Get the absolute path of the a file in the remote host */
    private String getRemoteAbsolutePath(String remotePath) throws Exception {
        try {
            return resolveHomeDir(remotePath);
        } catch (IOException ex) {
            throw new Exception(
                "Problem get absolute path for file : " + remotePath +
                ", error: " + ex.getMessage(), ex);
        }
    }

    private String resolveHomeDir(String remotePath)
        throws IOException {

        if (remotePath.startsWith(LINUX_HOME_SIGN)) {
            StringBuffer out = new StringBuffer();
            StringBuffer err = new StringBuffer();
            executeCommand("pwd", out, err);
            return remotePath.replaceAll(LINUX_HOME_SIGN,
                                         out.toString().trim());
        }
        return remotePath;
    }

    /*
     * Copy a file or a directory from remote machine to local machine via SFTP
     *
     * @param remoteSource the file path in remote machine
     * @param localTarget the destination directory in local machine
     * @param isRecursive indicate whether copy files recursively or not
     * @throws Exception
     */
    private void sftp(String remoteSource,
                      File localTarget,
                      boolean isRecursive)
        throws Exception {

        try (SftpClient sftp =
             SftpClientFactory.instance().createSftpClient(session)) {

            remoteSource = resolveHomeDir(remoteSource);

            /* Get attribute of remote file */
            Attributes attrs = sftp.stat(remoteSource);
            if (attrs.isDirectory()) {
                /* Iterate the remote file when it is a directory */
                String topFileName = getDirectoryLastName(remoteSource);

                /*
                 * Iterate the remote file path and get all files under this
                 * directory, except jdb files
                 */
                if (topFileName == null) {
                    iterateDirectory(sftp, remoteSource, localTarget,
                                     FILTERS, isRecursive);
                } else {
                    iterateDirectory(sftp, remoteSource,
                                     new File(localTarget, topFileName),
                                     FILTERS, isRecursive);
                }
            } else {
                /* Get file from remote machine to local machine */
                Path fileName = Paths.get(remoteSource).getFileName();
                getFile(sftp, remoteSource, fileName.toString(), localTarget);
            }
        } catch (IOException ioe) {
            throw new Exception(
                "Problem sftp file : " + remoteSource +
                ", error: " + ioe.getMessage(), ioe);
        }
    }

    /* Get last name of the specified full directory path of remote machine */
    private String getDirectoryLastName(String directory) {
        String lastName = null;
        if (directory == null || directory.equals("")) {
            return null;
        }

        /* root directory return it directly */
        if (directory.equals("/")) {
            return directory;
        }

        if (directory.endsWith("/")) {
            directory = directory.substring(0, directory.length() - 1);
        }

        int index = directory.lastIndexOf("/");
        if (index > 0) {
            lastName = directory.substring(index + 1);
        }

        return lastName;
    }

    /*
     * Iterate and copy all files under a specified directory from remote
     * machine to local target directory.
     *
     * @param channel Sftp channel
     * @param directory remote directory path
     * @param targetDir local target directory
     * @param filterPatterns filename patterns for files that should be excluded
     * @param isRecursive indicates whether to iterate the directory recursively
     * and to iterate its children directories or not
     * @throws IOException SFTP remote files errors
     */
    private void iterateDirectory(SftpClient sftp,
                                  String directory,
                                  File targetDir,
                                  String[] filteredPatterns,
                                  boolean isRecursive)
        throws IOException {

        /* create new directory when it does not exist */
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }

        /* List all files under directory in remote machine */
        Iterable<DirEntry> entries = sftp.readDir(directory);

        /*
         * Iterate all files under directory. If the listed file is a directory
         * and call iterateDirectory recursively; if the listed file is a file,
         * call getFile to get file from remote machine
         */
        for (DirEntry entry : entries) {
            String name = entry.getFilename();

            /* ignore . and .. from DirEntry */
            if (name.equals(".") || name.equals("..")) {
                continue;
            }
            String fullName = directory + "/" + name;
            if (entry.getAttributes().isDirectory()) {
                /* Iterate the children directories */
                if (isRecursive) {
                    /*
                     * Filter current directory and parent directory and
                     * the path contain the filtered patterns
                     */
                    if (isFiltered(fullName)) {
                        continue;
                    }
                    iterateDirectory(sftp,
                                     fullName,
                                     new File(targetDir, entry.getFilename()),
                                     filteredPatterns, isRecursive);
                }
            } else {
                /*
                 * Get file from remote machine when the path of the file
                 * does not match filtered pattern
                 */
                if (!isFiltered(fullName)) {
                    getFile(sftp, fullName, name, targetDir);
                }
            }
        }
    }

    /* Check whether a path should be filtered or not. */
    private boolean isFiltered(String path) {
        for (String pattern : FILTERS) {
            /*
             * Convert the pattern to be fit for java regular expressions.
             * (.) sign mean any characters, so it should be replaced by (\.).
             * replaceAll accepts regular expressions, so (.) should be escaped
             * as (\\.) and (\.) should be escaped as (\\\\.).
             *
             * (*) in the pattern also should be converted as (.*) which is fit
             * for java regular expressions. replaceAll accepts regular
             * expressions, so (*) should be escaped as (\\*).
             */
            pattern = pattern.replaceAll("\\.", "\\\\.");
            pattern = pattern.replaceAll("\\*", ".\\*");
            if (path.matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Copy a file from remote file path to local target directory with the
     * given file name.
     *
     * @throws IOException SFTP remote files errors
     */
    private void getFile(SftpClient sftp,
                         String remoteFilePath,
                         String fileName,
                         File targetDir)
        throws IOException {

        /* Create a new folder if it does not exist */
        if (!targetDir.exists()) {
            targetDir.mkdirs();
        }

        /* Invoke get API to fetch file from remote machine */
        File targetFile = new File(targetDir, fileName);

        /*
         * Use SftpClient.read(String path) without passing the buffer size.
         * Since Apache Mina SSHD 2.5.0, specifying buffer size may cause
         * corruption when copying the remote file. [KVSTORE-665]
         */
        try (InputStream instream = sftp.read(remoteFilePath)) {
            Files.copy(instream, targetFile.toPath(),
                       StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /**
     * Get all log files of a SNA from remote machine.
     *
     * @param snaInfo the info of a SNA
     * @param outputdir directory to store the log files
     * @param isCompress determine whether to compress the log files
     * @return the directory contains the log files
     * @throws Exception SFTP remote files errors
     */
    public File getLogFiles(SNAInfo snaInfo,
                            String outputdir,
                            boolean isCompress)
        throws Exception {

        String localSNDir = snaInfo.getStoreName() + "_" +
                snaInfo.getStorageNodeName();
        File resultDir = new File(outputdir, localSNDir);
        String rootdir = snaInfo.getRootdir();
        String localKVRootDir = getDirectoryLastName(rootdir);

        getFileFromRemoteDirectory(rootdir,
                                   resultDir,
                                   resultDir.getParentFile(),
                                   localSNDir,
                                   isCompress);
        /*
         * Iterate all files in specified root directory and determine which
         * one is the configuration file
         */
        Set<String> set = new HashSet<String>();
        File localKVRoot = new File(resultDir, localKVRootDir);
        File[] files = localKVRoot.listFiles();
        for (File file : files) {
            try {
                if (file.isDirectory()) {
                    continue;
                }
                BootstrapParams bp = ConfigUtils.
                    getBootstrapParams(file, false /* check read only*/);

                /* Check whether explicit storage directories are set for SNs */
                for (String path : bp.getStorageDirPaths()) {
                    set.add(path);
                }
                /* Check whether explicit rnlog directories are set for SNs */
                for (String path : bp.getRNLogDirPaths()) {
                    set.add(path);
                }
                /* Check whether explicit admin directory is set for SNs */
                for (String path : bp.getAdminDirPath()) {
                    set.add(path);
                }
            } catch (Exception ignore) {
                /*
                 * Ignore this exception. This exception is used to check
                 * whether the files under kvroot are configuration XML files
                 */
            }
        }

        /*
         * Copy the log files in the mount points
         */
        for (String mountPoint : set) {
            String localStorageDirName = mountPoint.replace('/', '_');
            getFileFromRemoteDirectory(mountPoint,
                                       resultDir,
                                       resultDir,
                                       localStorageDirName,
                                       isCompress);

            /* Rename the storage directory to avoid overwrite */
            File originalDir = new File(resultDir,
                                        getDirectoryLastName(mountPoint));
            File targetDir = new File(resultDir, localStorageDirName);
            originalDir.renameTo(targetDir);
        }

        return resultDir;
    }

    private void getFileFromRemoteDirectory(String remotedir,
                                            File resultDir,
                                            File targetDir,
                                            String targetDirName,
                                            boolean isCompress)
        throws Exception {

        String remotedirName = getDirectoryLastName(remotedir);

        if (isCompress) {
            /* Generate command to zip log files in remote machine */
            String zipfileName = targetDirName + ".zip";
            String remoteZipPath = "/tmp/" + zipfileName;
            String command = "cd " + remotedir + "/.. && zip -r " +
                                remoteZipPath + " " + remotedirName;
            String filterPatterns = getFilterPatterns();
            if (!filterPatterns.isEmpty()) {
                command += " -x " + filterPatterns;
            }

            /* Zip log files in remote machine */
            StringBuffer out = new StringBuffer();
            StringBuffer err = new StringBuffer();
            boolean status = executeCommand(command, out, err);

            if (!status) {
                throw new Exception(
                    "Generate zip file failed. " +
                    "Please specify -nocompress flag to retry.");
            }

            /*
             * Get all log files need copy the whole directory and its
             * children directories
             */
            sftp(remoteZipPath, targetDir, true);

            /* Clear standard output and err buffer */
            out.setLength(0);
            err.setLength(0);

            /* Delete generated zip file in remote machine */
            executeCommand("rm -rf " + remoteZipPath, out, err);

            unzip(targetDir + File.separator + zipfileName,
                  resultDir.getAbsolutePath());

        } else {
            /* Call sftp method to get all log file from a remote machine */
            sftp(remotedir, resultDir, true);
        }
    }

    /**
     * Get the latency between the client and a remote host. The algorithm is
     * as follows:
     * 1. client records the begin time
     * 2. client executes a command to show the date in remote host
     * 3. client gets the result from remote host and record the end time
     * 4. Assumed that the time taken to send command to and get result from
     * remote host are same, so the latency follow the formula:
     * latency = (remote time - begin time) - delta time
     * delta time = (end time - begin time) / 2
     * @return the latency; return Long.MIN_VALUE when the remote host can not
     * support date +%s%N command.
     * @throws Exception command execution error
     */
    public long getTimeLatency() throws Exception {
        /* To get accurate result, to several times */
        long minLatency = Long.MAX_VALUE;

        for (int i=0; i<5; i++) {
            StringBuffer out = new StringBuffer();
            StringBuffer err = new StringBuffer();
            boolean status = executeCommand("date +%s%N", out, err);
            if (!status) {
                return 0;
            }

            long remoteTime = 0;
            try {
                remoteTime = Long.parseLong(out.toString().trim()) /
                        (1000 * 1000);
            } catch (NumberFormatException nfe) {
                return Long.MIN_VALUE;
            }

            long latency = remoteTime - (end - begin) / 2 - begin;
            if (latency < minLatency) {
                minLatency = latency;
            }
        }

        return minLatency;
    }

    /**
     * Get the JDK version information from remote hosts by executing
     * "java -XshowSettings:properties".
     *
     * @return the JDK version and JDK vendor
     * @throws Exception command execution error
     */
    public JavaVersionVerifier getJavaVersion()
        throws Exception {

        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        executeCommand("java -XshowSettings:properties", out, err);

        String settings = err.toString().trim();
        List<String> settingList = Arrays.asList(settings.split("\n"));
        String vendor = null;
        String version = null;
        for (String item : settingList) {
            if (item.contains("java.vendor = ")) {
                vendor = item.replace("java.vendor = ", "").trim();
            }

            if (item.contains("java.version = ")) {
                version = item.replace("java.version = ", "").trim();
            }
        }

        JavaVersionVerifier verifier = new JavaVersionVerifier();
        verifier.setJKDVersionInfo(vendor, version);
        return verifier;
    }

    /**
     * Get the network connectivity status from the connected host to others
     * in the list by executing "ping -c 1 <host>"
     *
     * @param list the list contains the others hosts
     * @return the map contains the network connectivity status
     * @throws Exception command execution error
     */
    public Map<SNAInfo, Boolean> getNetWorkStatus(List<SNAInfo> list)
        throws Exception {

        /* Distinct host in SNA info list */
        Map<InetAddress, SNAInfo> ipSNAMap = new HashMap<>();

        /* Get IP Address of other hosts */
        for (SNAInfo si : list) {
            ipSNAMap.put(si.getIP(), si);
        }

        /*
         * Execute ping command in the connected host to ping other hosts to
         * determine whether the network is connected between them
         */
        Map<SNAInfo, Boolean> map = new HashMap<SNAInfo, Boolean>();
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        for (Map.Entry<InetAddress, SNAInfo> entry : ipSNAMap.entrySet()) {
            String command = "ping -c 1 " + entry.getValue().getHost();
            boolean status = executeCommand(command, out, err);
            map.put(entry.getValue(), status);
        }
        return map;
    }

    /**
     * Get all configuration files in the specified root directory of the
     * connected host. This method does:
     * - zip SNA root directory
     * - sftp zip to given local directory
     * - remove remote zip file
     * - unzip and find configuration files
     *
     * @param snaInfo SNA info
     * @param saveFolder local directory to save files
     * @return the list of the local path of the configuration files
     * @throws Exception command execution errors
     */
    public List<File> getConfigFile(SNAInfo snaInfo, File saveFolder)
        throws Exception {

        if (!saveFolder.exists()) {
            saveFolder.mkdirs();
        }

        String rootdir = snaInfo.getRootdir();
        String zipfileName = saveFolder.getName() + ".zip";
        String remoteZipPath = "/tmp/" + zipfileName;
        String command = "cd " + rootdir + " && zip " + remoteZipPath + " ./*";
        String filterPatterns = getFilterPatterns();
        if (!filterPatterns.isEmpty()) {
            command += " -x " + filterPatterns;
        }

        /* Zip log files in remote machine */
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        boolean status = executeCommand(command, out, err);

        if (!status) {
            throw new Exception("Generate zip file failed");
        }

        String remoteRoot = getRemoteAbsolutePath(snaInfo.getRootdir());
        /* Fetch generated zip file to local machine */
        sftp(remoteZipPath, saveFolder, true);

        /* Clear standard output and err buffer */
        out.setLength(0);
        err.setLength(0);

        /* Delete generated zip file in remote machine */
        executeCommand("rm -rf " + remoteZipPath, out, err);

        unzip(saveFolder + File.separator + zipfileName,
              saveFolder.getAbsolutePath());

        new File(saveFolder + File.separator + zipfileName).delete();

        snaInfo.setRemoteRootdir(remoteRoot);

        /*
         * Iterate all files in specified root directory and determine which
         * one is the configuration file
         */
        List<File> list = new ArrayList<File>();
        File[] files = saveFolder.listFiles();
        Map<String, String> securityDirMap = new HashMap<>();
        for (File file : files) {
            try {
                if (file.isDirectory()) {
                    continue;
                }
                BootstrapParams bp = ConfigUtils.
                    getBootstrapParams(file, false /* check read only*/);

                /*
                 * Check whether the configuration file indicates to enable
                 * security or not
                 */
                if (bp.getSecurityDir() != null &&
                        !bp.getSecurityDir().isEmpty()) {
                    /*
                     * Copy security files when configuration file indicates to
                     * enable security. If the security folder has existed in
                     * local, no need to copy it
                     */
                    String dir = securityDirMap.get(bp.getSecurityDir());
                    if (dir == null) {
                        getSecurityFile(file.getParentFile(),
                                        snaInfo.getRootdir(),
                                        bp.getSecurityDir());
                        securityDirMap.put(bp.getSecurityDir(),
                                           bp.getSecurityDir());
                    }
                }
                list.add(file);
            } catch (Exception ignore) {
                /*
                 * Ignore this exception. This exception is used to check
                 * whether the files under kvroot are configuration XML files
                 */
            }
        }
        return list;
    }

    /*
     * Copy security folder from remote host. This method does:
     * - zip security directory of given kvroot in remote host
     * - sftp zip file to local target dir/securityDir
     * - remove remote zip file
     * - unzip local security zip file
     *
     * @throws Exception command execution errors
     */
    private void getSecurityFile(File targetDir,
                                 String remoteKvroot,
                                 String securityDir)
        throws Exception {

        String remoteSource;
        if (remoteKvroot.endsWith("/")) {
            remoteSource = remoteKvroot + securityDir;
        } else {
            remoteSource = remoteKvroot + "/" + securityDir;
        }

        String zipfileName = targetDir.getName() + "_" + securityDir +
                ".zip";
        String remoteZipPath = "/tmp/" + zipfileName;
        String command = "cd " + remoteSource + " && zip -r " + remoteZipPath +
                " ./*";

        String filterPatterns = getFilterPatterns();
        if (!filterPatterns.isEmpty()) {
            command += " -x " + filterPatterns;
        }

        /* Zip log files in remote machine */
        StringBuffer out = new StringBuffer();
        StringBuffer err = new StringBuffer();
        boolean status = executeCommand(command, out, err);

        if (!status) {
            throw new Exception("Generate zip file failed");
        }

        File saveFolder = new File(targetDir, securityDir);

        /* Fetch generated zip file to local machine */
        sftp(remoteZipPath, saveFolder, true);

        /* Clear standard output and err buffer */
        out.setLength(0);
        err.setLength(0);

        /* Delete generated zip file in remote machine */
        executeCommand("rm -rf " + remoteZipPath, out, err);

        unzip(saveFolder + File.separator + zipfileName,
              saveFolder.getAbsolutePath());

        new File(saveFolder + File.separator + zipfileName).delete();
    }

    /* Generate the filter patterns for zip command */
    private String getFilterPatterns() {
        /*
         * zip 2.31 needs to add \ before each *, so to make it compatible
         * with zip 2.31 and 3.0, we add \ before each *.
         */
        String patterns = "";
        for (String pattern : FILTERS) {
            StringBuilder sb = new StringBuilder();
            for (char c : pattern.toCharArray()) {
                if (c == '*') {
                    sb.append("\\*");
                } else {
                    sb.append(c);
                }
            }
            patterns += sb.toString() + " ";
        }
        return patterns.trim();
    }

    /*
     * Unzip a given zipFile to output directory
     * @throws Exception unzip errors
     */
    private void unzip(String zipPath, String outputdir)
        throws Exception {

        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(zipPath);
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            /* Iterate all files in zip file */
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();

                /* Create a new directory when the entry is directory in zip */
                if (entry.isDirectory()) {
                    new File(outputdir, entry.getName()).mkdirs();
                    continue;
                }

                /*
                 * Read the file from zip and write in local machine when it is
                 * a file
                 */
                BufferedInputStream bis = null;
                FileOutputStream fos = null;
                BufferedOutputStream bos = null;
                try {
                    bis = new BufferedInputStream(zipFile.
                                                  getInputStream(entry));
                    File file = new File(outputdir, entry.getName());

                    File parent = file.getParentFile();
                    if (parent != null && (!parent.exists())) {
                        parent.mkdirs();
                    }
                    fos = new FileOutputStream(file);
                    bos = new BufferedOutputStream(fos, ZIP_BUFFER_SIZE);

                    int count;
                    byte data[] = new byte[ZIP_BUFFER_SIZE];
                    while ((count = bis.read(data, 0, ZIP_BUFFER_SIZE)) != -1) {
                        bos.write(data, 0, count);
                    }
                } finally {
                    if (bos != null) {
                        bos.flush();
                        bos.close();
                    }

                    if (fos != null) {
                        fos.flush();
                        fos.close();
                    }

                    if (bis != null) {
                        bis.close();
                    }
                }
            }
        } catch (Exception ex) {
            throw new Exception(
                "Problem unzipping file: " + zipPath +
                ", error: " + ex.getMessage(), ex);
        } finally {
            if (zipFile != null) {
                zipFile.close();
            }
        }
    }

    /*
     * Execute command in remote machine via SSH
     *
     * @param command the command to be executed via SSH
     * @param out is the standard output of executed command
     * @param err is the standard error message of executed command
     * @return the status of execution, true if command is executed
     * successfully; or false
     *
     * @throws IOException command execution error
     */
    private boolean executeCommand(String command,
                                   StringBuffer out,
                                   StringBuffer err)
        throws IOException {

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ByteArrayOutputStream errStream = new ByteArrayOutputStream();
        boolean status = false;

        ChannelExec channel = null;
        try {
            /* Open execution channel */
             channel = session.createExecChannel(command);
             channel.setOut(outStream);
             channel.setErr(errStream);

            /*
             * Connect and execute command and record the begin time and
             * end time
             */
            begin = System.currentTimeMillis();
            channel.open().verify();
            channel.waitFor(EnumSet.of(ClientChannelEvent.CLOSED), 0);
            end = System.currentTimeMillis();

            /* The execution is successful when status code is 0 */
            if (channel.getExitStatus() != null &&
                channel.getExitStatus().intValue() == 0) {
                status = true;
            }
            out.append(outStream.toString());
            err.append(errStream.toString());
            return status;
        } catch (IOException ex) {
            throw new IOException("Problem executing command : " + command +
                                  ", error: " + ex.getMessage(), ex);
        } finally {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }

    private String promptPassword()
        throws IOException {

        String prompt = username + "@" + host + "'s password: ";
        ShellPasswordReader passwordReader = new ShellPasswordReader();
        try {
            return new String(passwordReader.readPassword(prompt));
        } catch (Exception e) {
            throw new IOException("Error input password", e);
        }
    }
}
