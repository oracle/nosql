/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.util.CreateStore;
import oracle.kv.util.recovery.RecoverConfig;

import org.junit.Test;

/**
 * RecoverConfig Utility testing
 */
public class RecoverConfigTest extends TestBase {

    private CreateStore createStore = null;

    @Override
    public void setUp()
        throws Exception {
        super.setUp();
        RegistryUtils.clearRegistryCSF();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
        LoggerUtils.closeAllHandlers();
    }

    /**
     * Bad Arguments RecoverConfig tests
     */

    @Test
    public void testRecoverConfigEmptyArgument()
            throws Exception {

        final String[] argv = {};
        final String message = "Empty argument list";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigNoInputFlag()
            throws Exception {

        final String[] argv = {"-target", "/tmp/config.zip"};
        final String message = "-input flag argument not specified";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigNoOutputFlag()
            throws Exception {

        final String[] argv = {"-input", "/tmp/config"};
        final String message = "-target flag argument not specified";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigNoInputArgument()
            throws Exception {

        final String[] argv = {"-input"};
        final String message = "input requires an argument";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigNoOutputArgument()
            throws Exception {

        final String[] argv = {"-input", "/tmp/config", "-target"};
        final String message = "-target requires an argument";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigEmptyInputArgument()
            throws Exception {

        final String[] argv = {"-input", "", "-target"};
        final String message = "Input directory name must not be empty";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigEmptyOutputArgument()
            throws Exception {

        final String[] argv = {"-input", "/tmp/config", "-target", ""};
        final String message = "Target path must not be empty";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigInvalidInputPathArgument()
            throws Exception {

        final String[] argv = {"-input", "tmp", "-target"};
        final String message = "Input directory must be an absolute path";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigInvalidOutputPathArgument()
            throws Exception {

        final String[] argv = {"-input", "/tmp/config", "-target", "tmp"};
        final String message ="Target path must be an absolute path";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigInvalidOutputArgument()
            throws Exception {

        final String[] argv = {"-input", "/tmp/config", "-target", "/tmp"};
        final String message ="Target path must end with '.zip'";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigWrongArgument()
            throws Exception {

        final String[] argv = {"-abc"};
        final String message = "-abc is not a supported option.";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigInputDirNotExist()
            throws Exception {

        final String[] argv = {"-input", "/tmp/input",
                               "-target", "/tmp/output/config.zip"};
        final File inputDir = new File("/tmp/input");
        if (inputDir.exists()) {
            FileUtils.deleteDirectory(inputDir);
        }

        final String message = "Specified admin directory " +
                               "/tmp/input" + " does not exist";
        validateRecoverConfigOutput(argv, message);
    }

    @Test
    public void testRecoverConfigInputDirEmpty()
            throws Exception {

        final String[] argv = {"-input", "/tmp/input",
                               "-target", "/tmp/output/config.zip"};
        final File inputDir = new File("/tmp/input");
        if (inputDir.exists()) {
            FileUtils.deleteDirectory(inputDir);
        }
        RecoverConfig.makeDir(inputDir);

        final String message = "Specified admin directory " +
                               "/tmp/input" +
                               " does not contain any jdb files";

        validateRecoverConfigOutput(argv, message);
        FileUtils.deleteDirectory(inputDir);
    }

    private void validateRecoverConfigOutput(String[] argument,
                                             String message) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(output);
        PrintStream originalSysErr = System.err;
        System.setErr(ps);
        try {
            assertFalse(RecoverConfig.main1(argument));
            assertTrue(output.toString().contains(message));
        } finally {
            System.setErr(originalSysErr);
        }
    }

    /**
     * RecoverConfig utility working test :
     * [] Do an initial deploy
     * [] Test a working run of the utility
     *  --Copy the admin jdb files over to new location
     *  --Run recoverconfig on copied admin jdb files
     *  --Check output Directory for generated config fies.
     */
    @Test
    public void testRunningRecoverConfig ()
            throws Exception {

        /* Start a 1x3 store */
        try {
            createStore = new CreateStore(kvstoreName,
                                          5240, // random port to start at
                                          3, // numSNs
                                          3, // RF
                                          3, // numPartitions
                                          1, // capacity
                                          CreateStore.MB_PER_SN,
                                          true, /* useThreads */
                                          null,
                                          true,
                                          SECURITY_ENABLE);
            createStore.start();

            /*
             * All SNs has admins. Take admin database files from first
             * SN. Copy files from admin environment into different
             * directory under kvstore.
             *
             * TODO : There is scope of improvement here to take Master
             * admin files only.
             */
            final CommandServiceAPI cs = createStore.getAdmin();
            final Parameters params = cs.getParameters();
            final StorageNodeId snId =
                cs.getTopology().getSortedStorageNodeIds().get(0);
            final StorageNodeParams snp = params.get(snId);
            final String adminDirName = snp.getRootDirPath() + "/" +
                                        kvstoreName +
                                        "/" + snId.getFullName() + "/" +
                                        "admin" +
                                        String.valueOf(
                                            snId.getStorageNodeId()) +
                                        "/" + "env" + "/";
            final File file = new File(adminDirName);
            if (file != null) {
                File[] files = file.listFiles();
                if(files.length > 0) {
                    logger.info("admin" +
                                String.valueOf(snId.getStorageNodeId()) +
                                " env directory is not empty");
                }

                /*
                 * Create an input directory with name inputDir under
                 * kvroot
                 */
                final File inputDir =
                    new File(snp.getRootDirPath() + "/" + "inputDir");
                RecoverConfig.makeDir(inputDir);

                /*
                 * Copy files from admin Env directory to adminInputDir
                 */
                for (File inputfile : files) {
                    FileUtils.copyFile(inputfile,
                        new File(inputDir, inputfile.getName()));
                }

                /*
                 * -target path will be kvroot/config.zip
                 */
                String outputDirName = snp.getRootDirPath() + "/" +
                                       "config.zip";

                /*
                 * Run recoverconfig utility on inputDirectory and
                 * check creation of config.xml for SNs and
                 * topologyJSON output in config.zip file
                 */
                String[] argv = {"-input", inputDir.toString(),
                                 "-target", outputDirName};
                String message = "Configuration information recovered "
                                 + "successfully at " + outputDirName;
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(output);
                System.setOut(ps);
                assertTrue(RecoverConfig.main1(argv));
                assertTrue(output.toString().contains(message));

                final File outputDir = new File(snp.getRootDirPath());

                /*
                 * Check for created config.zip file. Further
                 * check config.xml and topologyJSON in zip file
                 */
                checkOutputDirectory(outputDir);

                /*
                 * Delete inputDir and config.zip directories
                 */
                FileUtils.deleteDirectory(inputDir);
                assertTrue(FileUtils.deleteDirectory(new File(outputDirName)));
            }
        } finally {
            if (createStore != null) {
                createStore.shutdown();
            }
        }
    }

    private void checkOutputDirectory(File outputDir) throws Exception {

        File[] outputDirFiles = outputDir.listFiles();
        for (File outputFile : outputDirFiles) {
            if (outputFile.getName().equals("config.zip")) {
                checkZipDirectory(outputFile);
            }
        }
    }

    private void checkZipDirectory(File outputFile) throws Exception {
        int numTopologyOutput = 0;
        int numConfigXml = 0;
        Set <String> numFiles = new HashSet<>();

        try {
            ZipFile configFile = new ZipFile(outputFile.getAbsolutePath());
            Enumeration <? extends ZipEntry> configEntries =
                configFile.entries();
            while (configEntries.hasMoreElements()) {
                ZipEntry configEntry = configEntries.nextElement();
                String entryName = configEntry.getName();
                if (entryName.contains("config.xml")) {
                    numConfigXml++;
                } else if (entryName.contains("topologyoutput.json")){
                    numTopologyOutput++;
                }
                numFiles.add(entryName);
            }
            configFile.close();
        } catch (IOException e) {
           throw new Exception ("Error reading config.zip file" + e);
        }

        /*
         * Six config files will be generated. Three bootStrap admin
         * config files. Three SNs config files.
         */
        assertEquals(numConfigXml, 6);

        /*
         * One entry for topologyoutput.json document
         */
        assertEquals(numTopologyOutput, 1);

       /*
        * total number of file entries will be 10 under config.zip
        *
        *  $outputdir/recoverconfig/kvroot_sn2/security.policy
        *  $outputdir/recoverconfig/kvroot_sn2/config.xml
        *  $outputdir/recoverconfig/kvroot_sn2/kvstore/security.policy
        *  $outputdir/recoverconfig/kvroot_sn2/kvstore/sn2/config.xml
        *  $outputdir/recoverconfig/topologyoutput.json
        *  $outputdir/recoverconfig/kvroot_sn3/security.policy
        *  $outputdir/recoverconfig/kvroot_sn3/config.xml
        *  $outputdir/recoverconfig/kvroot_sn3/kvstore/security.policy
        *  $outputdir/recoverconfig/kvroot_sn3/kvstore/sn3/config.xml
        *  $outputdir/recoverconfig/kvroot_sn1/security.policy
        *  $outputdir/recoverconfig/kvroot_sn1/config.xml
        *  $outputdir/recoverconfig/kvroot_sn1/kvstore/security.policy
        *  $outputdir/recoverconfig/kvroot_sn1/kvstore/sn1/config.xml
        */
        assertEquals(numFiles.size(), 13);

        /*
         * TODO : We can verify the actual directory structure. But
         * currently we are just checking if right number of files are
         * generated.
         */
    }
}
