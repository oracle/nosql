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
package oracle.kv.impl.query.shell;

import static oracle.kv.impl.util.CommandParser.CONSISTENCY_TIME_FLAG;
import static oracle.kv.impl.util.CommandParser.HELPER_HOSTS_FLAG;
import static oracle.kv.impl.util.CommandParser.MASTER_SYNC_FLAG;
import static oracle.kv.impl.util.CommandParser.PERMISSIBLE_LAG_FLAG;
import static oracle.kv.impl.util.CommandParser.PRETTY_FLAG;
import static oracle.kv.impl.util.CommandParser.REPLICA_ACK_FLAG;
import static oracle.kv.impl.util.CommandParser.REPLICA_SYNC_FLAG;
import static oracle.kv.impl.util.CommandParser.STORE_FLAG;
import static oracle.kv.impl.util.CommandParser.TIMEOUT_FLAG;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.FaultException;
import oracle.kv.KVSecurityException;
import oracle.kv.KVVersion;
import oracle.kv.impl.api.query.PreparedDdlStatementImpl;
import oracle.kv.impl.api.query.PreparedStatementImpl;
import oracle.kv.impl.api.table.DDLGenerator;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.OutputMode;
import oracle.kv.impl.util.CommandParser;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.query.PreparedStatement;
import oracle.kv.util.shell.CommonShell;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellRCFile;
import oracle.kv.util.shell.ShowCommandBase;

/**
 * A shell utility to execute sql statement.
 */
public class OnqlShell extends CommonShell {

    static String RC_FILE_SECTION = "sql";
    static OutputMode QUERY_OUTPUT_MODE_DEF = OutputMode.JSON;

    private OutputFile outputFile = null;
    private PrintStream queryOutput;
    private OutputMode queryOutputMode = null;

    public static final String COMMAND_ARGS =
        CommandParser.getHelperHostUsage() + " " +
        CommandParser.getStoreUsage() + eolt +
        CommandParser.optional(CommandParser.getUserUsage()) + " " +
        CommandParser.optional(CommandParser.getSecurityUsage()) + eolt +
        CommandParser.optional(CommandParser.getTimeoutUsage()) + eolt +
        CommandParser.optional(CommandParser.getConsistencyUsage()) + eolt +
        CommandParser.optional(CommandParser.getDurabilityUsage()) + eolt +
        "[single command and arguments]";

    private static final ExecuteCommand executeCommand = new ExecuteCommand();

    static final String prompt = "sql-> ";
    static final String usageHeader = "Oracle NoSQL SQL Shell commands:" + eol;

    public static
        List<? extends ShellCommand> commands =
                       Arrays.asList(new ConnectStoreCommand(),
                                     new ConsistencyCommand(),
                                     new DebugCommand(),
                                     new DurabilityCommand(),
                                     new ExitCommand(),
                                     new HelpCommand(),
                                     new HiddenCommand(),
                                     new HistoryCommand(),
                                     new ImportCommand(),
                                     new LoadCommand(),
                                     new NamespaceCommand(),
                                     new PutCommand(),
                                     new OutputModeCommand(),
                                     new OutputCommand(),
                                     new PageCommand(),
                                     new RequestTimeoutCommand(),
                                     new ShowCommand(),
                                     new TimeCommand(),
                                     new VerboseCommand(),
                                     new VersionCommand());

    /*
     * The keywords whose following value will be masked with mask
     * character(*) in the command line history.
     */
    private static final String[] maskFlags = new String[] {"identified by"};

    public OnqlShell(InputStream input, PrintStream output) {
        super(input, output, maskFlags);
        Collections.sort(commands, new Shell.CommandComparator());
        setGeneralCommand(executeCommand);
        queryOutput = output;
    }

    @Override
    public void init() {
        if (!isNoConnect()) {
            try {
                connectStore();
            } catch (ShellException se) {
                output.println(se.getMessage());
                if (getDebug()) {
                    se.printStackTrace(output);
                }
            }
        } else {
            output.println("Not connected to a store.  Use the connect " +
            		       "command to connect.");
        }
    }

    @Override
    public void shutdown() {
        closeStore();
        closeOutputFile();
    }

    @Override
    public List<? extends ShellCommand> getCommands() {
        return commands;
    }

    @Override
    public String getPrompt() {
        return isNoPrompt() ? null : prompt;
    }

    @Override
    public String getUsageHeader() {
        return usageHeader;
    }

    @Override
    public boolean doRetry() {
        return false;
    }

    public void setQueryOutputFile(String file)
        throws ShellException {

        closeOutputFile();
        if (file == null) {
            queryOutput = output;
            outputFile = null;
        } else {
            try {
                outputFile = new OutputFile(file);
            } catch (IOException e) {
                throw new ShellException(e.getMessage());
            }
            queryOutput = outputFile.getPrintStream();
        }
    }

    private void closeOutputFile() {
        if (outputFile != null) {
            try {
                outputFile.close();
            } catch (IOException ignored) {
            }
            queryOutput = output;
        }
    }

    public String getQueryOutputFile(){
        if (outputFile != null) {
            return outputFile.getFileName();
        }
        return null;
    }

    public PrintStream getQueryOutput() {
        return queryOutput;
    }

    public void flushOutput()
        throws IOException {

        if (outputFile != null) {
            outputFile.flush();
        }
    }

    public void setQueryOutputMode(OutputMode mode) {
        queryOutputMode = mode;
    }

    public OutputMode getQueryOutputMode() {
        return (queryOutputMode == null) ?
            QUERY_OUTPUT_MODE_DEF : queryOutputMode;
    }

    @Override
    public void handleUnknownException(String line, Exception e) {
        output.print("Unknown exception " + e);
        super.handleUnknownException(line, e);
    }

    /*
     * A simple class to manage the output file for query result.
     */
    private static class OutputFile {

        private final String fileName;
        private final FileDescriptor fd;
        private final PrintStream outStream;

        OutputFile(String fileName)
            throws IOException {

            final File f = new File(fileName);
            if(!f.exists()) {
                f.createNewFile();
            }
            final FileOutputStream fos = new FileOutputStream(f, true);
            final PrintStream ps = new PrintStream(fos, true);
            fd = fos.getFD();
            outStream = ps;
            this.fileName = fileName;
        }

        PrintStream getPrintStream() {
            return outStream;
        }

        String getFileName() {
            return fileName;
        }

        void flush()
            throws IOException {

            outStream.flush();
            fd.sync();
        }

        void close()
            throws IOException {

            outStream.flush();
            outStream.close();
            fd.sync();
        }
    }

    /* Consistency command */
    public static class ConsistencyCommand extends ShellCommand {
        final static String NAME = "consistency";
        final static String LAG_DESC = PERMISSIBLE_LAG_FLAG + " <time_ms>";
        final static String TIMEOUT_DESC = TIMEOUT_FLAG + " <time_ms>";
        final static String TIME_SYNTAX =
            CONSISTENCY_TIME_FLAG + " " + LAG_DESC + " " + TIMEOUT_DESC;
        final static String consistencyNames;
        static {
            final Set<String> names = getConsistencyNames();
            final StringBuilder sb = new StringBuilder();
            for (String name : names) {
                if (sb.length() > 0) {
                    sb.append(" | ");
                }
                sb.append(name);
            }
            consistencyNames = sb.toString();
        }
        final static String SYNTAX = NAME + " " +
            CommandParser.optional(CommandParser.optional(consistencyNames) +
                                   " | " + eolt + "            " +
                                   CommandParser.optional(TIME_SYNTAX));

        final static String DESCRIPTION = "Configures the read consistency " +
            "used for this session";

        public ConsistencyCommand() {
            super(NAME, 4);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            boolean timeBased = false;
            int lagTime = -1;
            int timeout = -1;
            Consistency cons = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (timeBased) {
                    if (PERMISSIBLE_LAG_FLAG.equals(arg)) {
                        lagTime = parseInt(Shell.nextArg(args, i++, this));
                        if (lagTime <= 0) {
                            invalidArgument(arg);
                        }
                    } else if (TIMEOUT_FLAG.equals(arg)) {
                        timeout = parseInt(Shell.nextArg(args, i++, this));
                        if (timeout <= 0) {
                            invalidArgument(arg);
                        }
                    } else {
                        shell.unknownArgument(arg, this);
                    }
                } else {
                    if (CONSISTENCY_TIME_FLAG.equals(arg)) {
                        timeBased = true;
                    } else {
                        cons = getConsistency(arg);
                        if (cons == null) {
                            invalidArgument(arg);
                        }
                    }
                }
            }
            if (timeBased) {
                if (lagTime == -1) {
                    shell.requiredArg(PERMISSIBLE_LAG_FLAG, this);
                }
                if (timeout == -1) {
                    shell.requiredArg(TIMEOUT_FLAG, this);
                }
                cons = new Consistency.Time(lagTime, TimeUnit.MILLISECONDS,
                                            timeout, TimeUnit.MILLISECONDS);
            }
            final OnqlShell sqlShell = (OnqlShell)shell;
            if (cons != null) {
                sqlShell.setStoreConsistency(cons);
            } else {
                cons = sqlShell.getStoreConsistency();
            }
            return "Read consistency policy: " +
                OnqlShell.getConsistencyName(cons);
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* Durability command */
    public static class DurabilityCommand extends ShellCommand {

        final static String NAME = "durability";
        final static String MASTER_SYNC_DESC =
            MASTER_SYNC_FLAG + " <sync-policy>";
        final static String REPLICA_SYNC_DESC =
            REPLICA_SYNC_FLAG + " <sync-policy>";
        final static String REPLICA_ACK_DESC =
            REPLICA_ACK_FLAG + " <ack-policy>";
        final static String CUSTOM_DRUALITY_SYNTAX =
            MASTER_SYNC_DESC + " " + REPLICA_SYNC_DESC + " " + REPLICA_ACK_DESC;
        final static String durabilityNames;
        final static String syncPolicyNames;
        final static String ackPolicyNames;
        static {
            /* Collects name of Durability */
            final Set<String> names = getDurabilityNames();
            final StringBuilder sb = new StringBuilder();
            for (String name : names) {
                if (sb.length() > 0) {
                    sb.append(" | ");
                }
                sb.append(name);
            }
            durabilityNames = sb.toString();

            /* Collects name of Sync polices */
            sb.setLength(0);
            for (SyncPolicy p: SyncPolicy.values()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(p.name());
            }
            syncPolicyNames = sb.toString();

            /* Collects name of ReplicaACK polices */
            sb.setLength(0);
            for (ReplicaAckPolicy p: ReplicaAckPolicy.values()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(p.name());
            }
            ackPolicyNames = sb.toString();
        }
        final static String SYNTAX = NAME + " " +
            CommandParser.optional(CommandParser.optional(durabilityNames) +
                " | " + eolt + "           " +
                CommandParser.optional(CUSTOM_DRUALITY_SYNTAX ));

        final static String DESCRIPTION = "Configures the write durability " +
            "used for this session." + eolt + "<sync-policy>: " +
            syncPolicyNames + eolt + "<ack-policy>: " + ackPolicyNames;

        public DurabilityCommand() {
            super(NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            Durability durability = null;
            boolean customized = false;
            SyncPolicy masterSync = null;
            SyncPolicy replicaSync = null;
            ReplicaAckPolicy replicaAck = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (MASTER_SYNC_FLAG.equals(arg)) {
                    if (durability != null) {
                        invalidArgument(arg);
                    }
                    masterSync = getSyncPolicy(Shell.nextArg(args, i++, this));
                    if (!customized) {
                        customized = true;
                    }
                } else if (REPLICA_SYNC_FLAG.equals(arg)) {
                    if (durability != null) {
                        invalidArgument(arg);
                    }
                    replicaSync = getSyncPolicy(Shell.nextArg(args, i++, this));
                    if (!customized) {
                        customized = true;
                    }
                } else if (REPLICA_ACK_FLAG.equals(arg)) {
                    if (durability != null) {
                        invalidArgument(arg);
                    }
                    replicaAck = getAckPolicy(Shell.nextArg(args, i++, this));
                    if (!customized) {
                        customized = true;
                    }
                } else {
                    if (customized) {
                        shell.unknownArgument(arg, this);
                    }
                    durability = getDurability(arg);
                    if (durability == null) {
                        invalidArgument(arg);
                    }
                }
            }

            if (customized) {
                if (masterSync == null) {
                    shell.requiredArg(MASTER_SYNC_FLAG, this);
                }
                if (replicaSync == null) {
                    shell.requiredArg(REPLICA_SYNC_FLAG, this);
                }
                if (replicaAck == null) {
                    shell.requiredArg(REPLICA_ACK_FLAG, this);
                }
                durability = new Durability(masterSync,
                                            replicaSync,
                                            replicaAck);
            }

            final OnqlShell sqlShell = (OnqlShell)shell;
            if (durability != null) {
                sqlShell.setStoreDurability(durability);
            } else {
                durability = sqlShell.getStoreDurability();
            }
            return "Write durability policy: " +
                OnqlShell.getDurabilityName(durability);
        }

        private SyncPolicy getSyncPolicy(String policy)
            throws ShellException {

            try {
                return SyncPolicy.valueOf(policy.toUpperCase());
            } catch (IllegalArgumentException iae) {
                invalidArgument(policy);
            }
            return null;
        }

        private ReplicaAckPolicy getAckPolicy(String policy)
            throws ShellException {

            try {
                return ReplicaAckPolicy.valueOf(policy.toUpperCase());
            } catch (IllegalArgumentException iae) {
                invalidArgument(policy);
            }
            return null;
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* Timeout command */
    public static class RequestTimeoutCommand extends ShellCommand {

        final static String NAME = "timeout";
        final static String SYNTAX = NAME + " " +
            CommandParser.optional("<timeout_ms>");
        final static String DESCRIPTION = "Configures the request timeout " +
            "for this session";

        public RequestTimeoutCommand() {
            super(NAME, 5);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            int timeout;
            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                final String arg = args[1];
                timeout = parseInt(arg);
                if (timeout <= 0) {
                    invalidArgument(arg);
                }
                ((OnqlShell)shell).setRequestTimeout(timeout);
            }
            timeout = ((OnqlShell)shell).getRequestTimeout();
            return "Request timeout used: " +
                String.format("%,d", timeout) + "ms";
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* Output command */
    public static class OutputCommand extends ShellCommand {

        private final static String STDOUT = "stdout";
        private final static String FILE_DESC = "<file>";

        final static String NAME = "output";
        final static String SYNTAX = NAME + " " +
            CommandParser.optional(STDOUT + " | " + FILE_DESC);
        final static String DESCRIPTION =
            "Enables or disables output of query results to a file";

        public OutputCommand() {
            super(NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final OnqlShell sqlShell = ((OnqlShell)shell);
            if (args.length > 2) {
                shell.badArgCount(this);
            } else if (args.length > 1) {
                String path = args[1];
                if (STDOUT.equals(path)) {
                    sqlShell.setQueryOutputFile(null);
                } else {
                    sqlShell.setQueryOutputFile(path);
                }
            }
            final String outputFile = sqlShell.getQueryOutputFile();
            return "Query output " +
                ((outputFile != null)? "file is " + outputFile : "is stdout");
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /* Mode command */
    public static class OutputModeCommand extends ShellCommand {
        final static String NAME = "mode";

        private final static String MODE_COLUMN = "COLUMN";
        private final static String MODE_LINE = "LINE";
        private final static String MODE_JSON = "JSON";
        private final static String MODE_CSV = "CSV";

        final static String SYNTAX;
        static {
            final StringBuilder sb = new StringBuilder();
            final String[] modes = {MODE_JSON, MODE_COLUMN,
                                    MODE_LINE, MODE_CSV};
            for (String mode : modes) {
                if (sb.length() > 0) {
                    sb.append(" | ");
                }
                sb.append(mode.toUpperCase());
                if (mode.equals(MODE_JSON)) {
                    sb.append(" ");
                    sb.append(CommandParser.optional(PRETTY_FLAG));
                }
            }
            SYNTAX = NAME + " " + CommandParser.optional(sb.toString());
        }
        final static String DESCRIPTION =
            "Sets the output mode of query results.";

        public OutputModeCommand() {
            super(NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            OnqlShell sqlShell = (OnqlShell)shell;
            OutputMode mode = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (mode != null) {
                    invalidArgument(arg);
                }
                if (MODE_COLUMN.equals(arg.toUpperCase())) {
                    mode = OutputMode.COLUMN;
                } else if (MODE_LINE.equals(arg.toUpperCase())) {
                    mode = OutputMode.LINE;
                } else if (MODE_JSON.equals(arg.toUpperCase())) {
                    if (++i < args.length) {
                        arg = args[i];
                        if (arg.equals(PRETTY_FLAG)) {
                            mode = OutputMode.JSON_PRETTY;
                            continue;
                        }
                        invalidArgument(arg);
                    }
                    mode = OutputMode.JSON;
                } else if (MODE_CSV.equals(arg.toUpperCase())) {
                    mode = OutputMode.CSV;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (args.length > 1) {
                sqlShell.setQueryOutputMode(mode);
            } else {
                mode = sqlShell.getQueryOutputMode();
            }
            return "Query output mode is " + getModeDesc(mode);
        }

        private String getModeDesc(final OutputMode mode)
            throws ShellException {

            if (mode == OutputMode.COLUMN) {
                return MODE_COLUMN;
            } else if (mode == OutputMode.JSON) {
                return MODE_JSON;
            } else if (mode == OutputMode.JSON_PRETTY) {
                return "pretty " + MODE_JSON;
            } else if (mode == OutputMode.LINE) {
                return MODE_LINE;
            } else if (mode == OutputMode.CSV) {
                return MODE_CSV;
            }
            throw new ShellException("Invalid output mode: " + mode);
        }

        @Override
        public String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        public String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    /*
     * Version command.
     */
    public static class VersionCommand extends ShellCommand {

        final static String NAME = "version";
        final static String SYNTAX = NAME;
        final static String DESCRIPTION = "Display client version information.";

        VersionCommand() {
            super(NAME, 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            final StringBuilder sb = new StringBuilder();
            sb.append("Client version: ");
            sb.append(KVVersion.CURRENT_VERSION.getNumericVersionString());
            sb.append(eol);
            return sb.toString();
        }

        @Override
        protected String getCommandSyntax() {
            return SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return DESCRIPTION;
        }
    }

    public static class ShowCommand extends ShowCommandBase {

        private static final List<? extends SubCommand> subs =
            Arrays.asList(new ShowDdl(),
                          new ShowFaults(),
                          new ShowQuery());

        public ShowCommand() {
            super(subs);
        }

        static final class ShowQuery extends SubCommand {

            final static String NAME = "query";
            final static String SYNTAX = COMMAND + " " + NAME + " <statement>";
            final static String DESCRIPTION = "Displays the query plan for " +
                "a query.";

            public ShowQuery() {
                super(NAME, 3);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length < 2) {
                    shell.badArgCount(this);
                }

                final String stmt;
                if (args.length == 2) {
                    stmt = args[1];
                } else {
                    stmt = getCommandLine();
                }

                try {
                    ExecuteOptions options = new ExecuteOptions()
                        .setNamespace(((OnqlShell)shell).getNamespace());
                    PreparedStatement ps =
                        ((OnqlShell)shell).getStore().prepare(stmt, options);
                    if (ps instanceof PreparedDdlStatementImpl) {
                        invalidArgument(stmt + " is not a valid query " +
                            "statement.");
                    }
                    return ((PreparedStatementImpl)ps).getQueryPlan().display();
                } catch (IllegalArgumentException iae) {
                    throw new ShellException(iae.getMessage(), iae);
                } catch (FaultException | KVSecurityException e) {
                    throw new ShellException(e.getMessage(), e);
                }
            }

            @Override
            protected boolean isMultilineInput() {
                return true;
            }

            @Override
            protected String getCommandSyntax() {
                return SYNTAX;
            }

            @Override
            protected String getCommandDescription() {
                return DESCRIPTION;
            }
        }

        static class ShowDdl extends SubCommand {

            final static String NAME = "ddl";
            final static String SYNTAX = COMMAND + " " + NAME + " <table>";
            final static String DESCRIPTION =
                "Returns ddls for the specified table, ddls include table " +
                "and index(es) if any";

            public ShowDdl() {
                super(NAME, 3);
            }

            @Override
            public String execute(String[] args, Shell shell)
                throws ShellException {

                Shell.checkHelp(args, this);
                if (args.length != 2) {
                    shell.badArgCount(this);
                }

                String nameArg = args[1];

                OnqlShell sqlsh = ((OnqlShell) shell);
                TableAPIImpl api = (TableAPIImpl)sqlsh.getStore().getTableAPI();

                String namespace;
                String tname;

                namespace = NameUtils.getNamespaceFromQualifiedName(nameArg);
                if (namespace != null) {
                    tname = NameUtils.getFullNameFromQualifiedName(nameArg);
                } else {
                    namespace = sqlsh.getNamespace();
                    tname = nameArg;
                }

                try {

                    TableImpl tbl = (TableImpl)api.getTable(namespace, tname);
                    if (tbl == null) {
                        throw new IllegalArgumentException("Table '" + nameArg +
                            "' not found");
                    }

                    DDLGenerator gen = new DDLGenerator(tbl,
                                                        true, /* withIfNotExists */
                                                        api.getRegionMapper());
                    StringBuilder sb = new StringBuilder();
                    sb.append(gen.getDDL());
                    for (String ddl : gen.getAllIndexDDL()) {
                        sb.append(Shell.eol).append(ddl);
                    }
                    return sb.toString();
                } catch (Exception ex) {
                    throw new ShellException(ex.getMessage(), ex);
                }
            }

            @Override
            protected String getCommandSyntax() {
                return SYNTAX;
            }

            @Override
            protected String getCommandDescription() {
                return DESCRIPTION;
            }
        }
    }

    private class OnqlShellParser extends ShellParser {

        OnqlShellParser(String[] args,
                       String[] rcArgs,
                       String[] requiredFlags) {
            super(args, rcArgs, requiredFlags);
        }

        @Override
        public String getShellUsage() {
            return KVSQLCLI_USAGE_PREFIX + eolt + COMMAND_ARGS;
        }
    }

    public void parseArgs(String args[]) {
        final String[] requiredFlags =
            new String[]{ HELPER_HOSTS_FLAG, STORE_FLAG };
        final String[] rcArgs = ShellRCFile.readSection(RC_FILE_SECTION);
        ShellParser parser = new OnqlShellParser(args, rcArgs, requiredFlags);
        parser.parseArgs();
    }

    public static void main(String[] args) {
        OnqlShell shell = new OnqlShell(System.in, System.out);
        shell.parseArgs(args);
        shell.start();
        if (shell.getExitCode() != EXIT_OK) {
            System.exit(shell.getExitCode());
        }
    }
}
