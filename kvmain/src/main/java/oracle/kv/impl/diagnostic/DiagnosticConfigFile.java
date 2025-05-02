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

package oracle.kv.impl.diagnostic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * This class manages the diagnostic command's config file, which holds
 * descriptors of the SNs which are the command's targets.
 */

public class DiagnosticConfigFile {
    private String configDir;

    public DiagnosticConfigFile(String configDir) {
        this.configDir = configDir;
    }

    /**
     * Add an SNA Info into configuration file.
     *
     * @param snaInfo the info of SNA
     */
    public void add(SNAInfo snaInfo) throws Exception {
        List<SNAInfo> list = getAllSNAInfo();
        list.add(snaInfo);
        rewrite(list);
    }

    /**
     * Add one SNAInfo into configuration file
     * @param snaInfo the specified SNAinfo
     * @throws Exception
     */
    public void addOneSNAInfo(SNAInfo snaInfo) throws Exception {
        File file = new File(configDir, DiagnosticConstants.CONFIG_FILE_NAME);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException ex) {
                throw new Exception("Problem creating " + file + ": " +
                                                ex.getMessage());
            }
        }

        BufferedWriter bw = null;

        try {
            bw = new BufferedWriter(new FileWriter(file, true));
            bw.write(snaInfo.toString());
            bw.newLine();

        } catch (Exception ex) {
            throw new Exception("Problem writing file " + file + ": " +
                                                ex.getMessage());
        } finally {
            try {
                if (bw != null)
                    bw.close();
            } catch (IOException ex) {
                throw new Exception("Problem closing file " + file + ": " +
                                                ex.toString());
            }
        }
    }

    /**
     * Clear configuration file.
     */
    public void clear() throws Exception {
        FileWriter fw = null;
        File file = null;
        try {
            file = new File(configDir, DiagnosticConstants.CONFIG_FILE_NAME);
            /* No need to clear file when the file does not exist */
            if (!file.exists()) {
                return;
            }
            fw = new FileWriter(file);
            fw.close();
        } catch (IOException ex) {
            throw new Exception("Problem with file " + file + ": " +
                                                ex.getMessage());
        } finally {
            try {
                if (fw != null)
                    fw.close();
            } catch (IOException ex) {
                throw new Exception("Problem closing file " + file + ": " +
                                                ex.toString());
            }
        }
    }

    /**
     * Determine whether delete the SNAInfo from configuration file
     */
    private boolean isDelete(SNAInfo snaInfo, SNAInfo patternSNAInfo) {
        boolean shouldBeDeleted = false;
        if (patternSNAInfo.getStoreName() != null &&
                !patternSNAInfo.getStoreName().isEmpty()) {
            if (!snaInfo.getStoreName().equals(patternSNAInfo.getStoreName())) {
                return false;
            }
            shouldBeDeleted = true;
        }

        if (patternSNAInfo.getStorageNodeName() != null &&
                !patternSNAInfo.getStorageNodeName().isEmpty()) {
            if (!snaInfo.getStorageNodeName().
                    equals(patternSNAInfo.getStorageNodeName())) {
                return false;
            }
            shouldBeDeleted = true;
        }

        if (patternSNAInfo.getHost() != null &&
                !patternSNAInfo.getHost().isEmpty()) {
            if (!snaInfo.getHost().equals(patternSNAInfo.getHost())) {
                return false;
            }
            shouldBeDeleted = true;
        }

        if (patternSNAInfo.getSSHUser() != null &&
                !patternSNAInfo.getSSHUser().isEmpty()) {
            if (!snaInfo.getSSHUser().equals(patternSNAInfo.getSSHUser())) {
                return false;
            }
            shouldBeDeleted = true;
        }

        if (patternSNAInfo.getRootdir() != null &&
                !patternSNAInfo.getRootdir().isEmpty()) {
            if (!snaInfo.getRootdir().equals(patternSNAInfo.getRootdir())) {
                return false;
            }
            shouldBeDeleted = true;
        }

        /*
         * Return this variable when this method does not return in
         * the previous statement.
         */
        return shouldBeDeleted;
    }

    /**
     * Delete SNAs from configuration file.
     */
    public int delete(SNAInfo patternSNAInfo) throws Exception {
        /* store the number of deleted SNA info from configuration file */
        int numberOfDeleted = 0;

        List<SNAInfo> keepSNAInfoList = new ArrayList<SNAInfo>();
        File file = new File(configDir, DiagnosticConstants.CONFIG_FILE_NAME);

        if (!file.exists()) {
            throw new FileNotFoundException("Cannot find SN target file " +
                            file +  ". Please use the diagnostics setup " +
                            "command to create a target list.");
        }

        List<SNAInfo> allSNAInfoList = getAllSNAInfo();
        for (SNAInfo snaInfo : allSNAInfoList) {
            /*
             * Check all SNA info in configuration and determine which ones
             * should be deleted
             */
            if (!isDelete(snaInfo, patternSNAInfo)) {
                keepSNAInfoList.add(snaInfo);
            } else {
                numberOfDeleted++;
            }
        }

        /* Return directly when no SNA info should be deleted */
        if (numberOfDeleted == 0) {
            return numberOfDeleted;
        }

        /* Write all lines should be keep in configuration file */
        rewrite(keepSNAInfoList);

        return numberOfDeleted;
    }

    /**
     * Get all SNAs from configuration file.
     * @throws Exception
     */
    public List<SNAInfo> getAllSNAInfo() throws Exception {
        List<SNAInfo> snaList = new ArrayList<SNAInfo>();
        BufferedReader br = null;
        File file = new File(configDir, DiagnosticConstants.CONFIG_FILE_NAME);
        try {
            try {
                br = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                /* Return empty list when configuration file is not found */
                return snaList;
            }

            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isEmpty()) {
                    SNAInfo snaInfo = new SNAInfo(line);
                    snaList.add(snaInfo);
                }
            }
            return snaList;
        } catch (Exception ex) {
            throw new Exception("Invalid configuration file " + file + ": " +
                    ex.getMessage());
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception ex) {
                throw new Exception("Problem closing file " + file + ": " +
                        ex.toString());
            }
        }
    }

    /**
     * Write the all SNAInfo into configuration file
     * @param snaInfoList SNA info list
     * @throws Exception
     */
    public void rewrite(List<SNAInfo> snaInfoList) throws Exception {
        clear();
        for (SNAInfo snaInfo : snaInfoList) {
            addOneSNAInfo(snaInfo);
        }
    }

    /**
     * Get the absolute path of configuration file.
     * @return absolute path of configuration file
     */
    public String getFilePath() {
        File file = new File(configDir, DiagnosticConstants.CONFIG_FILE_NAME);
        return file.getAbsolutePath();
    }

    /**
     * Verify whether the content of configuration file is valid or not.
     * @throws Exception
     */
    public void verify() throws Exception {
        File file = new File(configDir, DiagnosticConstants.CONFIG_FILE_NAME);

        /*
         * Map to store frequency of SNA descriptors and it is used to check
         * whether there are duplicated SNA descriptors in configuration file.
         */
        Map<SNAInfo, Integer> frequencyMap = new HashMap<SNAInfo, Integer>();

        /*
         * Map to store the duplicated SNA descriptors in configuration file
         */
        Map<SNAInfo, List<SNAInfo>> duplicatedMap =
                new HashMap<SNAInfo, List<SNAInfo>>();
        List<SNAInfo> snaInfoList;
        boolean isDuplicated = false;

        if (!file.exists()) {
            throw new FileNotFoundException("Cannot find file " + file);
        }

        /* Get all SNA info from configuration file */
        snaInfoList = getAllSNAInfo();

        /* Can not find SNA info in configuration file throw exception */
        if (snaInfoList.size() == 0) {
            throw new Exception("\n" + file +
                        " should not be empty. Please specify\n" +
                        "-host, -port or use diagnostics -setup "+
                        "to specify SN targets and populate the file");
        }

        /* Iterate all SNA info to find duplicated ones */
        for (SNAInfo checkedSNAInfo : snaInfoList) {
            /* Record the frequency of SNA descriptors */
            Integer frequency = frequencyMap.get(checkedSNAInfo);
            if (frequency == null) {
                frequencyMap.put(checkedSNAInfo, 1);

                List<SNAInfo> list = new ArrayList<SNAInfo>();
                list.add(checkedSNAInfo);
                duplicatedMap.put(checkedSNAInfo, list);
            } else {
                frequencyMap.put(checkedSNAInfo, frequency + 1);

                List<SNAInfo> list = duplicatedMap.get(checkedSNAInfo);
                list.add(checkedSNAInfo);
            }
        }

        /*
         * There is existing duplicated lines when frequency of
         * descriptors > 1
         */
        String duplicatedLines = "";
        for (Map.Entry<SNAInfo, Integer> entry : frequencyMap.entrySet()) {
            if (entry.getValue() > 1) {
                isDuplicated = true;

                /* Aggregate the duplicated lines into a variable */
                SNAInfo key = entry.getKey();

                List<SNAInfo> list = duplicatedMap.get(key);
                duplicatedLines += "\n";
                for (SNAInfo snaInfo : list) {
                    duplicatedLines += snaInfo.getSNAInfo() + "\n";
                }
            }
        }

        /* Find duplicated SNA info and throw exception */
        if (isDuplicated) {
            throw new Exception("Duplicated lines in configuration " +
                                    "file: " + duplicatedLines);
        }
    }
}

