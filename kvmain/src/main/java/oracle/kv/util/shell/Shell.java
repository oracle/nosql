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

package oracle.kv.util.shell;

import static oracle.kv.impl.util.CommandParser.DEBUG_FLAG;
import static oracle.kv.impl.util.CommandParser.HIDDEN_FLAG;
import static oracle.kv.impl.util.CommandParser.JSON_FLAG;
import static oracle.kv.impl.util.CommandParser.JSON_V1_FLAG;
import static oracle.kv.impl.util.CommandParser.VERBOSE_FLAG;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import oracle.kv.KVSecurityException;
import oracle.kv.impl.admin.CommandJsonUtils;
import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.util.CommandParser;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.ErrorMessage;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.ObjectNode;

/**
 *
 * Simple framework for a command line shell.  See CommandShell.java for a
 * concrete implementation.
 *
 * The shell supports the following global states:
 *      verbose
 *      debug
 *      hidden
 *      json
 *
 * In general, the global states can be toggled between on and off by commands
 * with the same name. They can also be set on a per-command (local) basis by
 * specifying a flag with that name on the command line. The get methods for
 * the state will return true if the either the global or local state is true.
 *
 */
public abstract class Shell {
    protected final InputStream input;
    protected final PrintStream output;
    private ShellInputReader inputReader = null;
    protected final CommandHistory history;
    protected int exitCode;
    protected boolean showDeprecated = false;
    private VariablesMap shellVariables = null;
    protected Stack<ShellCommand> stCurrentCommands = null;
    protected boolean isSecured = false;
    private final Timer timer;

    /*
     * The command to handle the general command line if no matched command
     * is found using findCommand().
     */
    private ShellCommand generalCommand = null;

    public final static String tab = "\t";
    public final static String eol = System.getProperty("line.separator");
    public final static String eolt = eol + tab;

    public static final String INCLUDE_DEPRECATED_FLAG = "-include-deprecated";

    /*
     * These are somewhat standard exit codes from sysexits.h
     */
    public final static int EXIT_OK = 0;
    public final static int EXIT_USAGE = 64; /* usage */
    public final static int EXIT_INPUTERR = 65; /* bad argument */
    public final static int EXIT_UNKNOWN = 1; /* unknown exception */
    public final static int EXIT_NOPERM = 77; /* permission denied */

    private final static char MASK = '*';

    /*
     * This variable changes per-command which means that things must be
     * single-threaded, which they are at this time.  The command line
     * parsing consumes any "-verbose" flag and sets this variable for
     * access by commands.
     */
    private boolean verbose = false;

    /*
     * Set to true if the hidden -debug flag is specified, and causes printing
     * of debugging output (stacktraces).  This field also changes per-command,
     * like the verbose field.
     */
    private boolean debug = false;

    /*
     * Set to true of the -hidden flag is specified.
     */
    private boolean hidden = false;

    /*
     * This variable is toggled by the "verbose" command
     */
    private boolean globalVerbose = false;

    /*
     * This variable is toggled by the "debug" command
     */
    private boolean globalDebug = false;

    /*
     * Toggled by the "hidden" command.
     */
    private boolean globalHidden = false;

    /*
     * This is used to change the output from Shell into json format strings.
     * Once it is set to true, the output of command will also be in JSON
     * format if the command supports JSON format result.
     */
    protected boolean globalJson = false;

    /*
     * This variable changes per-command which turns the output of command
     * execution into json format strings if the command supports JSON format
     * result.
     */
    protected boolean json = false;

    protected boolean jsonV1 = false;

    /*
     * This variable is set using "timer [on | off]" command, if it is true
     * then the elapsed time of execution of command is measured and printed
     * out.
     */
    private boolean timing = false;

    /*
     * This is used to terminate the interactive loop
     */
    private boolean terminate = false;

    /*
     * This is the ending character of command line.
     */
    private final static char LINE_TERMINATOR = ';';

    /*
     * Line continuation character
     */
    private final static char LINE_JOINER = '\\';

    /*
     * The comment mark string
     */
    public final static String COMMENT_MARK = "#";

    /*
     * The flag indicates if disable the event designator of JLine.
     */
    private final boolean disableJlineEventDesignator;

    /*
     * The flags with which the value specified need to mask with '*' in
     * command line history.
     */
    private final String[] maskFlags;

    /*
     * These must be implemented by specific shell classes
     */
    public abstract List<? extends ShellCommand> getCommands();

    public abstract String getPrompt();

    public abstract String getUsageHeader();

    public abstract void init();

    public abstract void shutdown();

    /*
     * Concrete implementation
     */
    public Shell(InputStream input, PrintStream output) {
        this(input, output, true);
    }

    public Shell(InputStream input,
                 PrintStream output,
                 boolean disableJlineEventDesignator) {
        this(input, output, disableJlineEventDesignator, null);
    }

    /**
     * The disableJlineEventDesignator indicates if disable the Jline history
     * event designator functionality that use characters like '!' or '^' as
     * as the indicator for the event designator:
     *
     * http://www.gnu.org/software/bash/manual/html_node/Event-Designators.html
     */
    public Shell(InputStream input,
                 PrintStream output,
                 boolean disableJlineEventDesignator,
                 String[] maskFlags) {
        this.input = input;
        this.output = output;
        history = new CommandHistory();
        stCurrentCommands = new Stack<ShellCommand>();
        shellVariables = new VariablesMap();
        timer = new Timer();
        this.disableJlineEventDesignator = disableJlineEventDesignator;
        this.maskFlags = maskFlags;
    }

    public String getUsage() {
        String usage = getUsageHeader();
        for (ShellCommand cmd : getCommands()) {
            if ((!getHidden() && cmd.isHidden()) ||
                (!showDeprecated() && cmd.isDeprecated())) {
                continue;
            }

            String help = cmd.getCommandName();
            if (help != null) {
                usage += tab + help + eol;
            }
        }
        return usage;
    }

    public void setShowDeprecated(boolean showDeprecated) {
        this.showDeprecated = showDeprecated;
    }

    public boolean showDeprecated() {
        return showDeprecated;
    }

    public void prompt() {
        String prompt = getPrompt();
        if (prompt != null) {
            output.print(prompt);
        }
    }

    /* Push a current command to stack. */
    public void pushCurrentCommand(ShellCommand command) {
        stCurrentCommands.push(command);
    }

    /* Get the current command on top of stack. */
    public ShellCommand getCurrentCommand() {
        if (stCurrentCommands.size() > 0) {
            return stCurrentCommands.peek();
        }
        return null;
    }

    /* Pop the current command on the top of stack. */
    public void popCurrentCommand() {
        stCurrentCommands.pop();
    }

    /* Return the customized propmt string of the current command. */
    public String getCurrentCommandPropmt() {
        ShellCommand command = getCurrentCommand();
        if (command == null) {
            return null;
        }

        Iterator<ShellCommand> it = stCurrentCommands.iterator();
        StringBuilder sb = new StringBuilder();
        while (it.hasNext()) {
            ShellCommand cmd = it.next();
            if (cmd.getPrompt() != null) {
                if (sb.length() > 0) {
                    sb.append(".");
                }
                sb.append(cmd.getPrompt());
            }
        }
        if (sb.length() > 0) {
            sb.append("-> ");
        }
        return sb.toString();
    }

    /* Store a variable. */
    public void addVariable(String name, Object value) {
        shellVariables.add(name, value);
    }

    /* Get the value of a variable. */
    public Object getVariable(String name) {
        return shellVariables.get(name);
    }

    /* Get all variables. */
    public Set<Entry<String, Object>> getAllVariables() {
        return shellVariables.getAll();
    }

    /* Remove a variable. */
    public void removeVariable(String name) {
        shellVariables.remove(name);
    }

    /* Remove all variables. */
    public void removeAllVariables() {
        shellVariables.reset();
    }

    public boolean doRetry() {
        return false;
    }

    public boolean handleShellException(String line, ShellException se) {
        /* do one retry */
        if (doRetry()) {
            return true;
        }

        final CommandResult cmdResult = se.getCommandResult();
        if (se instanceof ShellHelpException) {
            history.add(line, null);
            displayResultReport(line, cmdResult,
                ((ShellHelpException) se).getVerboseHelpMessage());
            exitCode = EXIT_USAGE;
            return false;
        } else if (se instanceof ShellUsageException) {
            history.add(line, null);
            ShellUsageException sue = (ShellUsageException) se;
            displayResultReport(line, cmdResult,
                                sue.getMessage() +
                                System.getProperty("line.separator") +
                                sue.getVerboseHelpMessage());
            exitCode = EXIT_USAGE;
            return false;
        } else if (se instanceof ShellArgumentException) {
            history.add(line, null);
            displayResultReport(line, cmdResult, se.getMessage());
            exitCode = EXIT_INPUTERR;
            return false;
        }

        history.add(line, se);
        String message = "Error handling command " + line + ": " +
                         se.getMessage();
        exitCode = EXIT_UNKNOWN;
        displayResultReport(line, cmdResult, message);
        if (getDebug()) {
            se.printStackTrace(output);
        }
        return false;
    }

    public void handleUnknownException(String line, Exception e) {
        history.add(line, e);
        exitCode = EXIT_UNKNOWN;
        final CommandResult cmdResult =
            new CommandResult.CommandFails(e.getMessage(),
                                           ErrorMessage.NOSQL_5500,
                                           CommandResult.NO_CLEANUP_JOBS);
        displayResultReport(line, cmdResult,
                            e.getMessage() +
                            " (" + e.getClass().getName() + ")");
        if (getDebug()) {
            e.printStackTrace(output);
        }
    }

    /**
     * General handler of KVSecurityException. The default behavior is to log
     * the command and output error messages.
     *
     * @param line command line
     * @param kvse instance of AuthenticationRequiredException
     * @return true only if a retry is intentional
     */
    public boolean
        handleKVSecurityException(String line,
                                  KVSecurityException kvse) {
        history.add(line, kvse);
        final CommandResult cmdResult =
                new CommandResult.CommandFails(kvse.getMessage(),
                                               ErrorMessage.NOSQL_5100,
                                               CommandResult.NO_CLEANUP_JOBS);
        displayResultReport(line, cmdResult,
                            "Error handling command " + line + ": " +
                                kvse.getMessage());
        if (getDebug()) {
            kvse.printStackTrace(output);
        }
        exitCode = EXIT_NOPERM;
        return false;
    }

    public void verboseOutput(String msg) {
        if (verbose || globalVerbose) {
            output.println(msg);
        }
    }

    public void setTerminate() {
        terminate = true;
    }

    public boolean getTerminate() {
        return terminate;
    }

    public void setGeneralCommand(ShellCommand command) {
        generalCommand = command;
    }

    private ShellCommand getGeneralCommand() {
        return generalCommand;
    }

    /*
     * The primary loop that reads lines and dispatches them to the appropriate
     * command.
     */
    public void loop() {
        try {
            /* initialize input reader */
            inputReader = ShellInputReader.getReader(this);
            inputReader.setDefaultPrompt(getPrompt());
            final CommandLinesParser clp = new CommandLinesParser(this);

            LoopUntilTerminate:
            while (!terminate) {
                final String promptDef = inputReader.getDefaultPrompt();
                boolean multiLineInput = false;

                do {
                    String prompt = getCurrentCommandPropmt();
                    if (multiLineInput) {
                        final int len = (prompt != null) ?
                            prompt.length() : promptDef.length();
                        prompt = String.format("%" + len + "s", "-> ");
                    }

                    final String line;
                    try {
                        line = inputReader.readLine(prompt);
                    } catch (IOException ioe) {
                        echo("Exception reading input: " + ioe + Shell.eol);
                        continue;
                    }

                    /*
                     * If read empty line (enter Ctrl-D), then terminate the
                     * loop or multi-line input mode.
                     */
                    if (line == null) {
                        if (multiLineInput) {
                            clp.reset();
                            output.println();
                            break;
                        }
                        break LoopUntilTerminate;
                    }

                    try {
                        clp.appendLine(line);
                    } catch (Exception e) {
                        final String[] commands = clp.getCommands();
                        assert(commands.length == 1);
                        handleExecuteException(commands[0], e);
                        clp.reset();
                        break;
                    }
                    if (!multiLineInput) {
                        multiLineInput = true;
                    }
                } while (!clp.complete());

                /* Execute command(s) */
                String[] commands = clp.getCommands();
                if (commands != null) {
                    for (String command: commands) {
                        execute(command);
                    }
                }
                clp.reset();
            }
        } finally {
            inputReader.shutdown();
            shutdown();
        }
    }

    public void println(String msg) {
        output.println(msg);
    }

    /*
     * Encapsulates runLine in try/catch blocks for calls from external tools
     * that construct Shell directly.  This is also used by loop().  This
     * function trims leading/trailing white space from the line.
     *
     * @param line The input command line to execute
     */
    public void execute(String line) {
        line = line.trim();
        if (line.length() == 0) {
            return;
        }
        try {
            runLine(line);
        } catch (Exception e) {
            handleExecuteException(line, e);
        }
    }

    private void handleExecuteException(String command, Exception e) {
        try {
            if (e instanceof KVSecurityException) {
                final KVSecurityException kvse = (KVSecurityException)e;
                /* Returns true to give a chance to retry the command once. */
                if (handleKVSecurityException(command, kvse)) {
                    runLine(command);
                }
            } else {
                throw e;
            }
        } catch (ShellException se) {
            /* Returns true if a retry is in order */
            if (handleShellException(command, (ShellException)e)) {
                execute(command);
            }
        } catch (Exception ex) {
            handleUnknownException(command, ex);
        }
    }

    public ShellCommand findCommand(String commandName) {
        for (ShellCommand command : getCommands()) {
            if (command.matches(commandName)) {
                return command;
            }
        }
        return null;
    }

    /*
     * Extract the named flag.  The flag must exist in the args
     */
    public static String[] extractArg(String[] args, String arg) {
        String[] retArgs = new String[args.length - 1];
        int i = 0;
        for (String s : args) {
            if (! arg.equals(s)) {
                retArgs[i++] = s;
            }
        }
        return retArgs;
    }

    /*
     * Checks the presence of flags common to all commands. If found the
     * "local" state is set to true and the flag is removed from the returned
     * argument list.
     */
    public String[] checkCommonFlags(String[] args) {
        verbose = false;
        debug = false;
        hidden = false;
        if (checkArg(args, VERBOSE_FLAG)) {
            verbose = true;
            args = extractArg(args, VERBOSE_FLAG);
        }
        if (checkArg(args, DEBUG_FLAG)) {
            debug = true;
            args = extractArg(args, DEBUG_FLAG);
        }
        if (checkArg(args, HIDDEN_FLAG)) {
            hidden = true;
            args = extractArg(args, HIDDEN_FLAG);
        }
        return args;
    }

    /*
     * Parse the -json and -json-v1 flag.
     * Find the command first, if the command has its own parser of JSON flags,
     * do not set the shell level JSON value.
     * If the command does not have own JSON override, check and extract out
     * the -json and -json-v1 flag, so that command's own parser will not
     * report unknown flag error.
     */
    protected String[] checkJson(String[] args) throws ShellException {
        json = false;
        jsonV1 = false;
        if (args == null || args.length == 0) {
            return args;
        }
        final ShellCommand command = findCommand(args[0]);
        if (command != null && command.overrideJsonFlag()) {
            return args;
        }
        String[] retArgs = args;
        json = checkArg(args, JSON_FLAG);
        jsonV1 = checkArg(args, JSON_V1_FLAG);
        if (json && jsonV1) {
            throw new ShellArgumentException(
                "cannot specify -json and -json-v1 together");
        }
        if (json) {
            retArgs = extractArg(args, JSON_FLAG);
        } else if (jsonV1) {
            retArgs = extractArg(args, JSON_V1_FLAG);
        }
        return retArgs;
    }

    /*
     * Parse a single line.  Treat "#" as comments
     */
    public String[] parseLine(String line) {
        return parseLine(line, false);
    }

    /*
     * Parse a single line.  Treat "#" as comments
     *
     * @param line The input line to parse
     * @param checkQuotesMatch Indicates if check the quotes occur in pair.
     *        If the quote character is checked to not occur in pair, then
     *        throw an exception.
     *
     * @return the array of string tokens
     */
    protected String[] parseLine(String line, boolean checkQuotesMatch) {
        int tokenType;
        List<String> words = new ArrayList<String>();
        StreamTokenizer st =
            new StreamTokenizer(new StringReader(adjustLineToParse(line)));

        st.resetSyntax();
        st.whitespaceChars(0, ' ');
        st.wordChars('!', 255);
        st.quoteChar('"');
        st.quoteChar('\'');
        st.commentChar('#');

        while (true) {
            try {
                tokenType = st.nextToken();
                if (tokenType == StreamTokenizer.TT_WORD) {
                    words.add(st.sval);
                } else if (tokenType == '\'' || tokenType == '"') {
                    String sVal = st.sval;
                    words.add(sVal);
                    if (words.size() > 1 && checkQuotesMatch) {
                        /*
                         * Check if the quotes occurred in pair, 2 kinds of
                         * mismatch cases are:
                         *  1. command -arg "
                         *  2. command -arg "xxx
                         *
                         * Way to check for above cases:
                         * The next token is EOF of line, the body of quoted
                         * string value is empty string or the body of quoted
                         * string is not empty but the end character of line
                         * is not quote character.
                         */
                        if (st.nextToken() == StreamTokenizer.TT_EOF) {
                            String quote = String.valueOf((char)tokenType);
                            if (sVal.length() == 0 || !line.endsWith(quote)) {
                                throw new RuntimeException("Except to found " +
                                            quote + " after " + st.sval +
                                            ", but not found");
                            }
                        }
                        /*
                         * Push back the current token to continue
                         * the parse work.
                         */
                        st.pushBack();
                    }
                } else if (tokenType == StreamTokenizer.TT_NUMBER) {
                    echo("Unexpected numeric token!" + eol);
                } else {
                    break;
                }
            } catch (IOException e) {
                break;
            }
        }
        return words.toArray(new String[words.size()]);
    }

    /**
     * Adjust the command line:
     *  - Add escape character '\\' before '\n' in the quoted string.
     */
    private String adjustLineToParse(String line) {
        StringBuilder sb = new StringBuilder(line);
        boolean inQuotes = false;
        char quote = 0;
        int pos = 0;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (!inQuotes) {
                if (ch == '\'' || ch == '\"') {
                    quote = ch;
                    inQuotes = true;
                }
            } else {
                if (ch == quote) {
                    inQuotes = false;
                } else {
                    if (ch == '\n') {
                        sb.insert(pos++, '\\');
                    }
                }
            }
            pos++;
        }
        return sb.toString();
    }

    /**
     * An exception class that encapsulates command line parse error that
     * thrown by parseLine. Use locally.
     */
    protected class ParseLineException extends ShellException  {
        private static final long serialVersionUID = 1L;
        public ParseLineException(String msg) {
            super(msg);
        }
    }

    public void runLine(String line)
        throws ShellException {

        runLine(line, false);
    }

    private void runLine(String line, boolean checkQuotesMatch)
        throws ShellException {

        exitCode = EXIT_OK;
        if (line.length() > 0 && !isComment(line)) {
            String[] splitArgs;
            try {
                splitArgs = parseLine(line, checkQuotesMatch);
            } catch (RuntimeException re) {
                throw new ParseLineException(re.getMessage());
            }
            String commandName = splitArgs[0];
            final boolean timerEnabled = getTimer();
            if (timerEnabled) {
               timer.begin();
            }
            String result = run(commandName, splitArgs, line);
            if (result != null) {
                output.println(result);
            }
            if (timerEnabled) {
                timer.end();
                output.println(timer.toString());
            }
            history.add(line, null);
        }
    }

    public String run(String commandName, String[] args)
        throws ShellException {

        return run(commandName, args, null);
    }

    protected String run(String commandName, String[] args, String line)
        throws ShellException {

        ShellCommand command = null;
        String[] cmdArgs = null;

        command = getCurrentCommand();
        if (command != null) {
            cmdArgs = new String[args.length + 1];
            cmdArgs[0] = command.getCommandName();
            System.arraycopy(args, 0, cmdArgs, 1, args.length);
        } else {
            command = findCommand(commandName);
            cmdArgs = args;
        }

        final ShellCommand genCommand = getGeneralCommand();

        cmdArgs = checkJson(cmdArgs);

        if (command != null) {
            cmdArgs = checkCommonFlags(cmdArgs);
            try {
                final String result = command.execute(cmdArgs, this, line);
                exitCode = command.getExitCode();
                return result;
            } catch (CommandNotFoundException cnfe) {
                /*
                 * No sub command found, run with general command if it is
                 * provided, otherwise throw the exception.
                 */
                if (genCommand == null) {
                    throw cnfe;
                }
            }
        } else {
            if (genCommand == null) {
                throw new ShellArgumentException("Could not find command: " +
                    commandName + eol + getUsage());
            }
        }

        /*
         * Use general command to execute the line, the passing in argument
         * is the whole command line.
         */
        final String cmdLine = (line != null) ? line : joinWithSpace(args);
        final String result = genCommand.execute(new String[]{cmdLine}, this);
        exitCode = genCommand.getExitCode();
        return result;
    }

    /*
     * Returns a string that consists of a array of strings joined with space
     * character.
     */
    private String joinWithSpace(final String[] args) {
        final StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append(arg);
        }
        return sb.toString();
    }

    public boolean getVerbose() {
        return (verbose || globalVerbose);
    }

    public void setVerbose(boolean val) {
        globalVerbose = val;
    }

    public boolean toggleVerbose() {
        globalVerbose = !globalVerbose;
        return globalVerbose;
    }

    /** Returns whether to print debugging output. */
    public boolean getDebug() {
        return (debug || globalDebug);
    }

    void setDebug(boolean val) {
        globalDebug = val;
    }

    public boolean toggleDebug() {
        globalDebug = !globalDebug;
        return globalDebug;
    }

    public boolean getHidden() {
        return (hidden || globalHidden);
    }

    protected void setHidden(boolean val) {
        globalHidden = val;
    }

    protected boolean toggleHidden() {
        globalHidden = !globalHidden;
        return globalHidden;
    }

    /**
     * Returns whether to print result in JSON format
     */
    public boolean getJson() {
        return (json || globalJson || jsonV1);
    }

    public boolean getJsonV1() {
        return jsonV1;
    }

    public void setJson(boolean val) {
        globalJson = val;
    }

    public PrintStream getOutput() {
        return output;
    }

    public ShellInputReader getInput() {
        return inputReader;
    }

    /* Enable or disable time measurement */
    public void setTimer(boolean val) {
        timing = val;
    }

    /* Returns whether time measurement is enabled or not. */
    public boolean getTimer() {
        return timing;
    }

    public int getExitCode() {
        return exitCode;
    }

    public CommandHistory getHistory() {
        return history;
    }

    public static String nextArg(String[] args, int index, ShellCommand cmd)
        throws ShellException {

        if (++index < args.length) {
            return args[index];
        }
        throw new ShellUsageException
            ("Flag " + args[index-1] + " requires an argument", cmd, true);
    }

    public void unknownArgument(String arg, ShellCommand command)
        throws ShellException {

        String msg = "Unknown argument: " + arg;
        throw new ShellUsageException(msg, command);
    }

    public void badArgCount(ShellCommand command)
        throws ShellException {

        String msg = "Incorrect number of arguments for command: " +
            command.getCommandName();
        throw new ShellUsageException(msg, command, true);
    }

    public void badArgUsage(String arg, String info, ShellCommand command)
        throws ShellException {

        String msg = "Invalid usage of the " + arg +
            " argument to the command: " + command.getCommandName();
        if (info != null && !info.isEmpty()) {
            msg = msg + " - " + info;
        }
        throw new ShellUsageException(msg, command);
    }

    public void requiredArg(String arg, ShellCommand command)
        throws ShellException {

        String msg = "Missing required argument" +
            ((arg != null) ? " (" + arg + ")" : "")
            + " for command: " +
            command.getCommandName();
        throw new ShellUsageException(msg, command, true);
    }

    /**
     * Displays the command execution result report.  If json flag is specified,
     * a json format report will be displayed. Otherwise, the non-json
     * description will be displayed.
     *
     * @param command command line
     * @param cmdResult command execution result
     * @param nonJsonDesc non-json description
     */
    public void displayResultReport(String command,
                                    CommandResult cmdResult,
                                    String nonJsonDesc) {
        if (getJsonV1()) {
            output.println(toJsonReport(command, cmdResult));
            return;
        }
        if (getJson()) {
            output.println(
                ShellCommandResult.toJsonReport(command, cmdResult));
            return;
        }
        output.println(nonJsonDesc);
    }

    public boolean isJlineEventDesignatorDisabled() {
        return disableJlineEventDesignator;
    }

    public String[] getMaskFlags() {
        return maskFlags;
    }

    public static String makeWhiteSpace(int indent) {
        String ret = "";
        for (int i = 0; i < indent; i++) {
            ret += " ";
        }
        return ret;
    }

    /*
     * Retrieve command syntax for admin web service OPTIONS.
     */
    public static String getCommandSyntax(ShellCommand sc) {
        return sc.getCommandSyntax();
    }

    /*
     * Retrieve command description for admin web service OPTIONS.
     */
    public static String getCommandDescription(ShellCommand sc) {
        return sc.getCommandDescription();
    }

    /*
     * Look for -help or ? or -? in a command line.  This method makes it easy
     * for commands to accept -help or related flags later in the command line
     * and interpret them as help requests.
     */
    public static void checkHelp(String[] args, ShellCommand command)
        throws ShellException {

        for (String s : args) {
            if (isHelpFlag(s)) {
                throw new ShellHelpException(command);
            }
        }
    }

    static boolean isHelpFlag(final String flag) {
        final String sl = flag.toLowerCase();
        return  (sl.equals("-help") ||
                 sl.equals("help") ||
                 sl.equals("?") ||
                 sl.equals("-?"));
    }

    /*
     * Return true if the named argument is in the command array.
     * The arg parameter is expected to be in lower case.
     */
    public static boolean checkArg(String[] args, String arg) {
        for (String s : args) {
            String sl = s.toLowerCase();
            if (sl.equals(arg)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Return the item following the specified flag, e.g. the caller may be
     * looking for the argument to a -name flag.  Return null if it does not
     * exist.
     */
    public static String getArg(String[] args, String arg) {
        boolean returnNext = false;
        for (String s : args) {
            if (returnNext) {
                return s;
            }
            String sl = s.toLowerCase();
            if (sl.equals(arg)) {
                returnNext = true;
            }
        }
        return null;
    }

    public static boolean matches(String inputName,
                                  String commandName) {
        return matches(inputName, commandName, 0);
    }

    public static boolean matches(String inputName,
                                  String commandName,
                                  int prefixMatchLength) {

        if (inputName.length() < prefixMatchLength) {
            return false;
        }

        if (prefixMatchLength > 0) {
            String match = inputName.toLowerCase();
            return (commandName.toLowerCase().startsWith(match));
        }

        /* Use the entire string for comparison */
        return commandName.toLowerCase().equals(inputName.toLowerCase());
    }

    public static String toJsonReport(String command,
                                      CommandResult cmdResult) {

        return CommandJsonUtils.getJsonResultString(command, cmdResult);
    }

    /**
     * Returns true if the line is comment.
     */
    public static boolean isComment(final String line) {
        return line.startsWith(COMMENT_MARK);
    }

    /**
     * Output status information during command execution in non-json mode. It
     * differs from verboseOutput() in that the message will be output even in
     * non-verbose mode.
     */
    public void echo(String msg) {
        if (!getJson()) {
            output.print(msg);
        }
    }

    /**
     * Returns a line in which the security information are masked with *, the
     * security information is specified with the flag in maskFlags.
     */
    static String toHistoryLine(String line, String[] maskFlags) {

        assert(line != null);
        if (line.length() == 0 || maskFlags == null) {
            return line;
        }

        final StringBuilder sb = new StringBuilder();
        String s = line;
        for (String flag : maskFlags) {
            boolean ignoreCase = !flag.startsWith("-");
            flag = " " + flag + " "; /* match whole word */
            int pos = 0;
            /*
             * Mask the value(s) of specified flag, it is possible that the
             * flag occurs in command line more than one time
             *
             *  e.g. flag is "-secret"
             *    pwdfile secret -file user2.pwd -secret ABcd__1234
             *  =>pwdfile secret -file user2.pwd -secret **********
             */
            while (pos < s.length() &&
                   (pos = findString(s, flag, pos, ignoreCase)) >= 0) {

                pos += flag.length();
                if (pos < s.length()) {
                    sb.setLength(0);
                    pos = maskWord(sb, s, pos);
                    s = sb.toString();
                }
            }
        }

        return s;
    }

    /**
     * String.IndexOf() in case sensitive or insensitive manner.
     */
    private static int findString(String line,
                                  String str,
                                  int fromIndex,
                                  boolean ignoreCase) {
        return ignoreCase ?
                line.toLowerCase().indexOf(str.toLowerCase(), fromIndex) :
                line.indexOf(str, fromIndex);
    }

    /**
     * Replaces each character from the specified position to a delimiter with
     * mask character '*', the delimiter can be WHITE SPACE, quotes (" or ') or
     * terminator of whole command line semicolon (;), the new string is stored
     * into the passed in StringBuilder object.
     *
     * e.g. flag is -secret
     *   pwdfile secret ... -secret ABcd__1234
     * =>pwdfile secret ... -secret **********
     *
     *   pwdfile secret ... -secret 'AB"cd"__1234'
     * =>pwdfile secret ... -secret '************'
     *
     *   pwdfile secret ... -secret "ABcd__1234";
     * =>pwdfile secret ... -secret "**********";
     *
     *   pwdfile secret ... -secret ABcd__1234;
     * =>pwdfile secret ... -secret **********;
     */
    private static int maskWord(StringBuilder sb, String s, int index) {

        sb.append(s.substring(0, index));

        char stop = s.charAt(index);
        if (stop == '\'' || stop == '"') {
            sb.append(stop);
            index++;
        } else {
            stop = ' ';
        }

        while (index < s.length()) {
            char ch = s.charAt(index++);
            if (ch == stop || (index == s.length() && ch == ';')) {
                if (ch == '\'' || ch == '"') {
                    sb.append(ch);
                } else {
                    index--;
                }
                break;
            }
            sb.append(MASK);
        }

        int nextFrom = sb.length();
        if (index < s.length()) {
            sb.append(s.substring(index));
        }
        return nextFrom;
    }

    public static class LoadCommand extends ShellCommand {

        public LoadCommand() {
            super("load", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);
            String path = null;
            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if ("-file".equals(arg)) {
                    path = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }
            if (path == null) {
                shell.requiredArg("-file", this);
            }

            FileReader fr = null;
            BufferedReader br = null;
            String retString = null;
            try {
                final CommandLinesParser clp = new CommandLinesParser(shell);
                fr = new FileReader(path);
                br = new BufferedReader(fr);
                String line;

                LoopReadLine:
                while ((line = br.readLine()) != null &&
                       !shell.getTerminate() &&
                       shell.getExitCode() == Shell.EXIT_OK) {
                    try {
                        if (line.trim().isEmpty()) {
                            /*
                             * The empty line indicates the termination of
                             * command, so append line terminator ';'.
                             */
                            clp.appendLine(String.valueOf(LINE_TERMINATOR));
                        } else {
                            clp.appendLine(line);
                        }
                    } catch (Exception e) {
                        final String[] commands = clp.getCommands();
                        assert(commands.length == 1);
                        final String msg =
                            handleExecuteException(shell, commands[0], e);
                        /*
                         * If the returned 'msg' is not null, then the exception
                         * is handled and stop execution. Otherwise, continue
                         * to read next line and execute it.
                         */
                        if (msg != null) {
                            retString = msg;
                            break LoopReadLine;
                        }
                        clp.reset();
                        continue;
                    }

                    if (!clp.complete()) {
                        continue;
                    }

                    /* Execute commands if parsing is done. */
                    final String[] commands = clp.getCommands();
                    if (commands != null) {
                        retString = executeCommands(shell, commands);
                        if (retString != null) {
                            break LoopReadLine;
                        }
                    }
                    clp.reset();
                }

                /*
                 * If there are left command(s) not executed after the whole
                 * file has been read, handle them.
                 */
                if (retString == null && clp.hasCommand()) {
                    final String[] commands = clp.getCommands(false);
                    assert (commands != null);
                    retString = executeCommands(shell, commands);
                }
                exitCode = shell.getExitCode();
            } catch (IOException ioe) {
                exitCode = Shell.EXIT_INPUTERR;
                final String msg = "Failed to load file: " + path;
                if(!shell.getJson()) {
                    return msg;
                }
                CommandResult cmdResult = new CommandResult.CommandFails(
                    msg, ErrorMessage.NOSQL_5100,
                    CommandResult.NO_CLEANUP_JOBS);
                return Shell.toJsonReport(getCommandName(), cmdResult);
            } finally {
                if (fr != null) {
                    try {
                        fr.close();
                    } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }
            }
            if (shell.getJson()) {
                /* If JSON enable, don't output LoadCommand status. */
                return "";
            }
            return ((retString == null) ? "" : retString);
        }

        /**
         * Execute the commands, returns not-null message if one of commands
         * executed failed and the exception is handled, otherwise return null.
         */
        private String executeCommands(final Shell shell,
                                       final String[] commands) {

            for (String cmd: commands) {
                cmd = cmd.trim();
                try {
                    shell.runLine(cmd);
                } catch (Exception e) {
                    final String msg =
                        handleExecuteException(shell, cmd, e);
                    if (msg != null) {
                        return msg;
                    }
                }
            }
            return null;
        }

        /**
         * Handle the execution exception, returns not-null string if the
         * exception is handled.
         *  - Returns the error message if the exception is ShellException and
         *    handled, otherwise return null (retry case).
         *  - For other unknown exception, returns a empty string
         */
        private String handleExecuteException(Shell shell,
                                              String command,
                                              Exception e) {
            if (e instanceof ShellException) {
                /* stop execution if false is returned */
                if (!shell.handleShellException(command, (ShellException)e)) {
                    return "Script error in line \"" + command +
                            "\", ending execution";
                }
                return null;
            }
            /* Unknown exceptions will terminate the script */
            shell.handleUnknownException(command, e);
            return "";
        }

        @Override
        protected String getCommandSyntax() {
            return "load -file <path to file>";
        }

        @Override
        protected String getCommandDescription() {
            return
                "Load the named file and interpret its contents as a script " +
                "of commands" + eolt + "to be executed.  If any command in " +
                "the script fails execution will end.";
        }
    }

    public static class HelpCommand extends ShellCommand {

        public HelpCommand() {
            super("help", 2);
        }

        @Override
        protected boolean matches(String commandName) {
            return ("?".equals(commandName) ||
                    super.matches(commandName));
        }

        public String[] checkForDeprecatedFlag(String[] args, Shell shell) {
            String[] retArgs = args;
            shell.setShowDeprecated(false);
            if (checkArg(args, INCLUDE_DEPRECATED_FLAG)) {
                shell.setShowDeprecated(true);
                retArgs = extractArg(args, INCLUDE_DEPRECATED_FLAG);
            }
            return retArgs;
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            args = checkForDeprecatedFlag(args, shell);

            if (args.length == 1) {
                return shell.getUsage();
            }

            /* per-command help */
            String commandName = args[1];
            ShellCommand command = shell.findCommand(commandName);
            if (command != null) {
                return(command.getHelp(Arrays.copyOfRange(args, 1, args.length),
                                       shell));
            }
            return("Could not find command: " + commandName +
                   eol + shell.getUsage());
        }

        @Override
        public ShellCommandResult
            executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("help");
            scr.setDescription(execute(args, shell));
            return scr;
        }

        @Override
        protected String getCommandSyntax() {
            return "help [command [sub-command]] [-include-deprecated] " +
                   CommandParser.optional(CommandParser.JSON_FLAG);
        }

        @Override
        protected String getCommandDescription() {
            return "Print help messages.  With no arguments the top-level shell"
                    + " commands" + eolt + "are listed.  With additional "
                    + "commands and sub-commands, additional" + eolt
                    + "detail is provided. " + "Will list only those commands "
                    + "that are not" + eolt + "deprecated. To list "
                    + "the deprecated commands as well, use the"
                    + eolt + INCLUDE_DEPRECATED_FLAG + " flag.";
        }
    }

    public static class ExitCommand extends ShellCommand {

        public ExitCommand() {
            super("exit", 2);
        }

        @Override
        protected boolean matches(String commandName) {
            return super.matches(commandName) ||
                   Shell.matches(commandName, "quit", 2);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            shell.setTerminate();
            return "";
        }

        @Override
        public ShellCommandResult executeJsonOutput(String[] args, Shell shell)
            throws ShellException {
            shell.setTerminate();
            final ShellCommandResult scr =
                ShellCommandResult.getDefault("exit");
            scr.setDescription("exiting shell");
            return scr;
        }

        @Override
        protected String getCommandSyntax() {
            return "exit | quit " +
                   CommandParser.optional(CommandParser.JSON_FLAG);
        }

        @Override
        protected String getCommandDescription() {
            return "Exit the interactive command shell.";
        }
    }

    /*
     * Maintain command history.
     *
     * TODO: limit the size of the list -- requires a circular buffer.
     */
    public class CommandHistory {
        private final List<CommandHistoryElement> history1 =
            new ArrayList<CommandHistoryElement>(100);

        /**
         * Add a command to the history
         *
         * @param command the command to add
         * @param e Exception encountered on command if any, otherwise null
         */
        public void add(String command, Exception e) {
            String line = toHistoryLine(command, getMaskFlags());
            history1.add(new CommandHistoryElement(line, e));
        }

        /**
         * Gets the specified element in the history
         *
         * @param which the offset in the history array
         * @return the specified command
         */
        public CommandHistoryElement get(int which) {
            if (history1.size() > which) {
                return history1.get(which);
            }
            output.println("No such command in history at offset " + which);
            return null;
        }

        public int getSize() {
            return history1.size();
        }

        /**
         * Dumps the current history
         */
        public String dump(int from, int to) {
            from = Math.min(from, history1.size());
            to = Math.min(to, history1.size() - 1);
            String hist = "";
            for (int i = from; i <= to; i++) {
                hist += dumpCommand(i, false /* withFault */);
            }
            return hist;
        }

        /*
         * Caller verifies range
         */
        public boolean commandFaulted(int command) {
            CommandHistoryElement cmd = history1.get(command);
            return (cmd.getException() != null);
        }

        /*
         * Caller verifies range
         */
        public String dumpCommand(int command, boolean withFault) {
            CommandHistoryElement cmd = history1.get(command);
            String res = "";
            res = cmd.getCommand();
            if (withFault && cmd.getException() != null) {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                cmd.getException().printStackTrace(new PrintWriter(b, true));
                res += eolt + b.toString();
            }
            /*
             * The index of command are shown as 1-based index in output,
             * it is to be consistent with that in Jline command history,
             * then user can rerun a single command in history using !n,
             * n is 1-based index of command in the history.
             */
            return ((command + 1) + " " + res + eol);
        }

        public ObjectNode dumpCommandJson(int command, boolean withFault) {
            final ObjectNode top = JsonUtils.createObjectNode();
            CommandHistoryElement cmd = history1.get(command);
            String res = "";
            res = cmd.getCommand();
            top.put("index", command + 1);
            top.put("name", res);
            if (withFault && cmd.getException() != null) {
                ByteArrayOutputStream b = new ByteArrayOutputStream();
                cmd.getException().printStackTrace(new PrintWriter(b, true));
                top.put("exceptionStack", b.toString());
            }
            return top;
        }

        public String dumpFaultingCommands(int from, int to) {
            from = Math.min(from, history1.size());
            to = Math.min(to, history1.size() - 1);
            String hist = "";
            for (int i = from; i <= to; i++) {
                CommandHistoryElement cmd = history1.get(i);
                Exception e = cmd.getException();
                if (e != null) {
                    String res = "";
                    res = cmd.getCommand();
                    /*
                     * The index of command are shown as 1-based index in
                     * output, see details in above dumpCommand() method.
                     */
                    hist += ((i + 1) + " " + res + ": " + e.getClass() + eol);
                }
            }
            return hist;
        }

        public ObjectNode dumpFaultingCommandsJson(int from, int to) {
            final ObjectNode top = JsonUtils.createObjectNode();
            from = Math.min(from, history1.size());
            to = Math.min(to, history1.size() - 1);
            final ArrayNode faultCommandArray = top.putArray("faultCommands");
            for (int i = from; i <= to; i++) {
                CommandHistoryElement cmd = history1.get(i);
                Exception e = cmd.getException();
                if (e != null) {
                    String res = "";
                    res = cmd.getCommand();
                    final ObjectNode faultNode = JsonUtils.createObjectNode();
                    faultNode.put("index", i + 1);
                    faultNode.put("name", res);
                    faultNode.put("exceptionClass", e.getClass().toString());
                    faultCommandArray.add(faultNode);
                }
            }
            return top;
        }

        public Exception getLastException() {
            for (int i = history1.size() - 1; i >= 0; i--) {
                CommandHistoryElement cmd = history1.get(i);
                if (cmd.getException() != null) {
                    return cmd.getException();
                }
            }
            return null;
        }

        public String dumpLastFault() {
            for (int i = history1.size() - 1; i >= 0; i--) {
                CommandHistoryElement cmd = history1.get(i);
                if (cmd.getException() != null) {
                    return dumpCommand(i, true);
                }
            }
            return "";
        }

        public ObjectNode dumpLastFaultJson() {
            for (int i = history1.size() - 1; i >= 0; i--) {
                CommandHistoryElement cmd = history1.get(i);
                if (cmd.getException() != null) {
                    return dumpCommandJson(i, true);
                }
            }
            return null;
        }

        public void clear() {
            history1.clear();
        }
    }

    class CommandHistoryElement {
        String command;
        Exception exception;

        public CommandHistoryElement(String command, Exception exception) {
            this.command = command;
            this.exception = exception;
        }

        public String getCommand() {
            return command;
        }

        public Exception getException() {
            return exception;
        }
    }

    public static class CommandComparator implements Comparator<ShellCommand> {

        @Override
        public int compare(ShellCommand o1, ShellCommand o2) {
            return o1.getCommandName().compareTo(o2.getCommandName());
        }
    }

    /*
     * Maintain a HashMap to store variables.
     */
    public static class VariablesMap implements Cloneable {
        private final HashMap<String, Object> variablesMap =
            new HashMap<String, Object>();

        public void add(String name, Object value) {
            variablesMap.put(name, value);
        }

        public Object get(String name) {
            return variablesMap.get(name);
        }

        public Set<Entry<String, Object>> getAll() {
            return variablesMap.entrySet();
        }

        public void remove(String name) {
            if (variablesMap.containsKey(name)) {
                variablesMap.remove(name);
            }
        }

        public int size() {
            return variablesMap.size();
        }

        public void reset() {
            variablesMap.clear();
        }

        @Override
        public VariablesMap clone() {
            VariablesMap map = new VariablesMap();
            for (Map.Entry<String, Object> entry : variablesMap.entrySet()) {
                map.add(entry.getKey(), entry.getValue());
            }
            return map;
        }

        @Override
        public String toString() {
            String retString = "";
            for (Map.Entry<String, Object> entry: variablesMap.entrySet()) {
                retString += Shell.tab + entry.getKey() + ": " +
                             entry.getValue() + Shell.eol;
            }
            return retString;
         }
     }

     /**
      * A class used to parse input line(s) to command(s), it can deal with
      * below command style:
      *  - Single line command
      *  - Multi-line command joined by backslash '\'
      *  - Multi-line command with semicolon as terminator
      *  - Multiple commands with semicolon as command terminator
      *
      * 4 methods provided:
      *  1) appendLine(String line): Stores the new line to internal string
      *     buffer, check if command(s) are complete.
      *  2) complete(): Returns true if the input line(s) contains command(s)
      *     which are all complete, otherwise return false.
      *  3) getCommands(): Returns a array of parsed commands.
      *  4) reset(): Resets the CommandLinesParser to initial state.
      *
      * Generally, it can be used like below:
      *
      *     CommandLinesParser clp = new CommandLinesParser(shell);
      *     do {
      *         ...
      *         clp.appendLine(newLine);
      *     } while(!clp.complete())
      *
      *     String[] commands = clp.getCommands();
      *     ...
      *     clp.reset();
      */
    private static class CommandLinesParser {
        /* Help command */
        private static String HELP_COMMAND = "?";
        private static enum ParseState {
            SINGLE_LINE,
            MULTI_LINE_CONT,
            MULTI_LINE_TERM,
            PARSE_DONE_EXECUTED,
            PARSE_DONE,
        }
        private final Shell shell;
        private final StringBuilder sb;
        private ParseState state;

        CommandLinesParser(Shell shell) {
            this.shell = shell;
            sb = new StringBuilder();
            state = ParseState.SINGLE_LINE;
        }

        /*
         * Append the new line to internal string buffer, check if the
         * command line string matches any of below styles:
         *  - Single line command
         *  - Multi-line command joined by backslash '\'
         *  - Multi-line command with semicolon as terminator
         *  - Multiple commands with semicolon as command terminator
         *
         *  The checkComplete() method may be called to check if the command is
         *  complete, internally the command is executed, so the exception
         *  may be thrown out if the execution failed, the outer caller should
         *  handle the exception.
         */
        void appendLine(final String line)
            throws Exception {

            String command = line.trim();
            if (command.length() == 0) {
                if (state == ParseState.SINGLE_LINE) {
                    state = ParseState.PARSE_DONE_EXECUTED;
                }
                return;
            }

            /* Read "?", then terminate the parsing  */
            if (command.equalsIgnoreCase(HELP_COMMAND)) {
                sb.append(" ");
                sb.append(HELP_COMMAND);
                state = ParseState.PARSE_DONE;
                return;
            }
            final char ending = command.charAt(command.length() - 1);
            final boolean endWithCont = (ending == LINE_JOINER);
            final boolean endWithTerm = (ending == LINE_TERMINATOR);

            /* Append command to string buffer */
            if (endWithCont) {
                command = command.substring(0, command.length() - 1);
                if (!command.endsWith(" ")) {
                    command = command + " ";
                }
            }
            if (state == ParseState.MULTI_LINE_TERM) {
                sb.append("\n");
            }
            sb.append(command);

            /*
             * If the input line is ended with the line terminator , then
             * terminates the parsing.
             */
            if (endWithTerm) {
                state = ParseState.PARSE_DONE;
                return;
            }

            String cmdToExecute = null;
            switch (state) {
            case SINGLE_LINE:
                if (endWithCont) {
                    state = ParseState.MULTI_LINE_CONT;
                } else {
                    /*
                     * Parses the command line string, if it is single command,
                     * then need checking its completeness by executing it.
                     */
                    final String[] commands = parseCommandLines(sb.toString());
                    if (commands.length > 1) {
                        state = ParseState.MULTI_LINE_TERM;
                    } else {
                        cmdToExecute = commands[0];
                    }
                }
                break;
            case MULTI_LINE_CONT:
                if (!endWithCont) {
                    /*
                     * Parses the command line string, if it is single command,
                     * then need checking its completeness by executing it.
                     */
                    final String[] commands = parseCommandLines(sb.toString());
                    if (commands.length > 1) {
                        state = ParseState.MULTI_LINE_TERM;
                    } else {
                        cmdToExecute = commands[0];
                    }
                }
                break;
            default:
                break;
            }

            /* Check if the command is complete or not. */
            if (cmdToExecute != null) {
                try {
                    final boolean isCompleted = checkCompleted(cmdToExecute);
                    if (!isCompleted) {
                        state = ParseState.MULTI_LINE_TERM;
                    } else {
                        state = ParseState.PARSE_DONE_EXECUTED;
                    }
                } catch (Exception e) {
                    /* Throw the exception if execution of command failed. */
                    state = ParseState.PARSE_DONE;
                    throw e;
                }
            }
        }

        /* Returns true if the parsing state is done. */
        boolean complete() {
            return (state == ParseState.PARSE_DONE ||
                    state == ParseState.PARSE_DONE_EXECUTED);
        }

        /*
         * Returns the array of commands if parsing state is PARSE_DONE.
         */
        String[] getCommands() {
            return getCommands(true);
        }

        /*
         * Returns the array of commands.
         *
         * If parseDone is true, then returns the commands if parsing state is
         * PARSE_DONE. For none-complete states like SINGLE_LINE,
         * MULTI_LINE_TERM and MULTI_LINE_CONT, returns null. If state is
         * PARSE_DONE_EXECUTED, the command was executed so return null as well.
         *
         * If parseDone is false, then returns the commands whatever the parsing
         * state is.
         */
        String[] getCommands(boolean parseDone) {
            if (!parseDone || state == ParseState.PARSE_DONE) {
                return parseCommandLines(sb.toString());
            }
            return null;
        }

        /* Resets the state and string buffer to initial value. */
        void reset() {
            state = ParseState.SINGLE_LINE;
            sb.setLength(0);
        }

        boolean hasCommand() {
            return sb.length() > 0;
        }

        /*
         * Check if the input command is complete by executing the command,
         * the command is regarded as incomplete if caught below 2 exceptions:
         *   1) ParseLineException
         *   2) ShellUsageException and ShellUsageException.requireArgument()
         *      returns true.
         *
         * If execution failed, then throw exception.
         */
        private boolean checkCompleted(String command)
            throws Exception {

            if (isComment(command)) {
                return true;
            }
            /*
             * Check if the command line takes multiple lines input defaultly.
             */
            if (isMultilineCommand(command)) {
                return false;
            }

            try {
                shell.runLine(command, true);
                return true;
            } catch (ShellException se) {
                if (se instanceof ParseLineException) {
                    return false;
                }
                if ((se instanceof ShellUsageException) &&
                    ((ShellUsageException) se).requireArgument()) {
                    return false;
                }
                throw se;
            } catch (Exception e) {
                throw e;
            }
        }

        /**
         * Returns true if the command found to execute the command line takes
         * multiple line input defaultly.
         */
        private boolean isMultilineCommand(String command) {
            final String[] commandArgs = command.split(" ");
            int iArg = 0;
            ShellCommand cmd = shell.findCommand(commandArgs[iArg++]);
            if (cmd != null) {
                if (cmd instanceof CommandWithSubs) {
                    if (commandArgs.length == 1) {
                        return false;
                    }

                    cmd = ((CommandWithSubs)cmd)
                            .findCommand(commandArgs[iArg++]);
                    if (cmd == null) {
                        return false;
                    }
                }
                if (cmd.isMultilineInput()) {
                    if (iArg < commandArgs.length) {
                        if (isHelpFlag(commandArgs[iArg])) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }
            return (shell.getCurrentCommand() == null &&
                    (shell.getGeneralCommand() != null &&
                     shell.getGeneralCommand().isMultilineInput()));
        }

        /*
         * Split a single line into multiple command lines on the delimiter
         * semicolon ; but ignore the semicolon in single/double quotes:
         *
         * e.g.
         *      cmd1; cmd2 "xxx; yyy"; cmd3 'xxx; yyy'
         *  =>
         *      cmd1
         *      cmd2 "xxx; yyy"
         *      cmd3 'xxx; yyy'
         */
        private String[] parseCommandLines(String line) {
            String pattern = ";(?=(?:[^'\"]|\"[^\"]*\"|'[^']*')*$)";
            return line.trim().replaceAll(";+$", "").split(pattern);
        }
    }

    private static class Timer {
        private long time;

        Timer() {
            time = 0;
        }

        void begin() {
            time = getWallClockTime();
        }

        void end() {
            time = getWallClockTime() - time;
        }

        private long getWallClockTime() {
            return System.currentTimeMillis();
        }

        @Override
        public String toString() {
            final String fmt = "\nTime: %,dsec %dms";
            long sec = TimeUnit.MILLISECONDS.toSeconds(time);
            long ms = time - TimeUnit.SECONDS.toMillis(sec);
            return String.format(fmt, sec, ms);
        }
    }
}
