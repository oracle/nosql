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

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.StringTokenizer;

import oracle.kv.util.shell.Shell.CommandHistory;

public class ShellInputReader {

    /* JLine 3 APIs */
    /* org.jline.reader.LineReader methods */
    private Method readLineMeth;
    private Method readLineWithMaskMeth;
    private Method setVariableMeth;
    private Method setOptMeth;
    private String histFileVar;
    private String histFileSizeVar;
    /* org.jline.terminal.Terminal methods */
    private Method getHeightMeth;
    private Method closeMeth;
    private Method resumeMeth;
    private Method pauseMeth;
    private Method isPausedMeth;
    /* org.jline.reader.History methods */
    private Method sizeMeth;
    private Method getMeth;
    private Method saveMeth;
    private Method attachMeth;
    /* A boolean flag that indicates if the terminal is in paused state */
    private boolean isTermPaused;
    /**
     * LineReader.readLine() may throw UserInterruptException and
     * EndOfFileException.
     */
    private Class<?> userInterruptException;
    private Class<?> endOfFileException;

    /* Default value for terminal height */
    private static final int TERMINAL_HEIGHT_DEFAULT = 25;

    /* Property name to disable JLine. */
    private static final String PROP_JLINE_DISABLE =
        "oracle.kv.shell.jline.disable";
    /* Property name of JLine history file. */
    private static final String PROP_HISTORY_FILE =
        "oracle.kv.shell.history.file";
    /* Property name of JLine history size. */
    private static final String PROP_HISTORY_SIZE =
        "oracle.kv.shell.history.size";
    /* Property name to disable using system console, used by unit test */
    private static final String PROP_SYS_CONSOLE_DISABLE =
        "oracle.kv.shell.sys.console.disable";

    private Object jReaderObj = null;
    private Object jFileHistoryObj = null;
    private Object jTermObj = null;
    private BufferedReader inputReader = null;
    private PrintStream output = null;
    private String prompt = "";

    /*
     * The singleton reader created with system standard input and output.
     *
     * Since 3.14.0, Jline falls back to dumb terminal if multiple system
     * terminal are created, which makes the most of Jline functionality
     * not working.
     */
    private static ShellInputReader sysReaderInstance;

    /**
     * Get a ShellInputReader with given input and output stream.
     *
     * @return the singleton system terminal if using system standard input
     * and output, otherwise create a new ShellInputReader with specified
     * input and output stream, mostly in unit tests.
     */
    public synchronized static ShellInputReader getReader(InputStream input,
                                                          PrintStream output) {
        if (input == System.in && output == System.out) {
            if (sysReaderInstance == null) {
                sysReaderInstance = new ShellInputReader(input, output);
            }
            return sysReaderInstance;
        }
        return new ShellInputReader(input, output);
    }

    /**
     * Get a ShellInputReader using input and output stream of given shell.
     *
     * @return the singleton system terminal if using system standard input
     * and output, otherwise create a new ShellInputReader with specified
     * input and output stream, mostly in unit tests.
     */
    public static ShellInputReader getReader(Shell shell) {
        if (shell.input == System.in && shell.output == System.out) {
            if (sysReaderInstance == null) {
                sysReaderInstance = new ShellInputReader(shell);
            }
            return sysReaderInstance;
        }
        return new ShellInputReader(shell);
    }

    /* Protected for unit test */
    protected ShellInputReader(InputStream input, PrintStream output) {
        this(input, output, null, true, null);
    }

    private ShellInputReader(Shell shell) {
        this(shell.input, shell.output, getHistoryFile(shell),
             shell.isJlineEventDesignatorDisabled(), shell.getMaskFlags());
        loadCommandHistory(shell);
    }

    private ShellInputReader(InputStream input,
                             PrintStream output,
                             File historyFile,
                             boolean disableExpandEvents,
                             String[] maskFlags) {
        initInputReader(input, output, historyFile,
                        disableExpandEvents, maskFlags);
        this.output = output;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void initInputReader(InputStream input,
                                 PrintStream output1,
                                 File historyFile,
                                 boolean disableExpandEvents,
                                 String[] maskFlags) {

        if (!isJlineCompatiblePlatform()) {
            inputReader = new BufferedReader(new InputStreamReader(input));
            return;
        }

        try {
            final Class<?> readerBuilder =
                Class.forName("org.jline.reader.LineReaderBuilder");
            final Class<?> reader =
                Class.forName("org.jline.reader.LineReader");
            final Class<?> termBuilder =
                Class.forName("org.jline.terminal.TerminalBuilder");
            final Class<?> term =
                Class.forName("org.jline.terminal.Terminal");
            final Class<?> dumbTerm =
                Class.forName("org.jline.terminal.impl.DumbTerminal");
            final Class<?> history =
                Class.forName("org.jline.reader.History");
            final Class<?> defaultHistory =
                Class.forName("org.jline.reader.impl.history.DefaultHistory");
            final Class<?> readerOpt =
                Class.forName("org.jline.reader.LineReader$Option");
            userInterruptException =
                Class.forName("org.jline.reader.UserInterruptException");
            endOfFileException =
                Class.forName("org.jline.reader.EndOfFileException");

            /* TerminalBuilder methods */
            final Method tbBuilder = termBuilder.getMethod("builder");
            final Method tbBuild = termBuilder.getMethod("build");
            final Method tbSystem =
                termBuilder.getMethod("system", boolean.class);
            final Method tbPaused =
                termBuilder.getMethod("paused", boolean.class);

            /* Terminal methods */
            getHeightMeth = term.getMethod("getHeight");
            closeMeth = term.getMethod("close");
            resumeMeth = term.getMethod("resume");
            pauseMeth = term.getMethod("pause");
            isPausedMeth = term.getMethod("paused");

            /* LineReaderBuilder methods */
            final Method lrbBuilder = readerBuilder.getMethod("builder");
            final Method lrbBuild = readerBuilder.getMethod("build");
            final Method lrbTerm = readerBuilder.getMethod("terminal", term);
            final Method lrbHistory =
                readerBuilder.getMethod("history", history);

            /* LineReader methods */
            readLineMeth = reader.getMethod("readLine", String.class);
            readLineWithMaskMeth =
                reader.getMethod("readLine", String.class, Character.class);
            setVariableMeth =
                reader.getMethod("setVariable", String.class, Object.class);
            setOptMeth = reader.getMethod("setOpt", readerOpt);

            /* History methods */
            sizeMeth = defaultHistory.getMethod("size");
            getMeth = defaultHistory.getMethod("get", int.class);
            saveMeth = defaultHistory.getMethod("save");
            attachMeth = defaultHistory.getMethod("attach", reader);

            /* Create a Terminal instance */
            Object termBuilderObj = tbBuilder.invoke(null);
            final boolean createSysTerm =
                (System.in == input) && (System.out == output1);
            if (createSysTerm) {
                /**
                 * Try to create a system terminal. If the input device is not
                 * a tty, or jna/jansi library does not exist in the classpath,
                 * jline would fall back to create a DumbTerminal.
                 */
                termBuilderObj = tbSystem.invoke(termBuilderObj, true);
                /**
                 * Initialize the terminal with paused state so that the input
                 * streams are not consumed until LineReader.readLine() is
                 * called.
                 */
                termBuilderObj = tbPaused.invoke(termBuilderObj, true);
                jTermObj = tbBuild.invoke(termBuilderObj);
            } else {
                /**
                 * As of jline-3.7.1, the terminal implementations that use the
                 * pump mechanism (PosixPtyTerminal and ExternalTerminal)
                 * would repeatedly throw IOExceptions that originate from
                 * the underlying input stream. This leads to an issue that
                 * once an IOException occurs, subsequent reads would always
                 * fail no matter the IOException is recoverable or not (see
                 * https://github.com/jline/jline3/issues/270). We have to
                 * use the DumbTerminal which does not depend on the pump
                 * mechanism to work around the issue. As the TerminalBuilder
                 * API cannot create a DumbTerminal instance with supplied
                 * input/output streams, we have to call the constructor
                 * directly.
                 */
                final Constructor<?> dumbTermCtor = dumbTerm.getConstructor(
                    new Class<?>[] {InputStream.class, OutputStream.class});
                jTermObj = dumbTermCtor.newInstance(input, output1);
                pauseMeth.invoke(jTermObj);
            }
            isTermPaused = (Boolean) isPausedMeth.invoke(jTermObj);

            Object lrbObj = lrbBuilder.invoke(null);
            lrbObj = lrbTerm.invoke(lrbObj, jTermObj);
            if (historyFile != null) {
                final Constructor<?> histCtor =
                    defaultHistory.getConstructor();
                jFileHistoryObj = histCtor.newInstance();
                final Object historyObj = (maskFlags == null) ?
                    jFileHistoryObj :
                    FileHistoryProxy.create(jFileHistoryObj, maskFlags);
                lrbObj = lrbHistory.invoke(lrbObj, historyObj);
            }

            /* Create a LineReader instance */
            jReaderObj = lrbBuild.invoke(lrbObj);
            if (historyFile != null) {
                final Field histFile = reader.getField("HISTORY_FILE");
                final Field histFileSize =
                    reader.getField("HISTORY_FILE_SIZE");
                histFileVar = (String) histFile.get(jReaderObj);
                histFileSizeVar = (String) histFileSize.get(jReaderObj);
                setReaderVariable(histFileVar, historyFile.getAbsolutePath());
                /* set history file size */
                setHistoryFileSize();

                // [#27810] for some reason the line reader builder invoke() doesn't call the history.attach() method,
                // so we call it manually here before the loadCommandHistory() method is called
                if(attachMeth != null) {
                    try {
                        invokeMethod(jFileHistoryObj, attachMeth, new Object[] {jReaderObj});
                    } catch (Exception ignored)  /* CHECKSTYLE:OFF */ {
                    } /* CHECKSTYLE:ON */
                }
            }

            /* Disable the event designators, it is enabled by default */
            if (disableExpandEvents) {
                final Enum<?>[] constants =
                    (Enum<?>[]) readerOpt.getEnumConstants();
                for (Enum<?> e : constants) {
                    if ("DISABLE_EVENT_EXPANSION".equals(e.name())) {
                        final Enum<?> option =
                            Enum.valueOf((Class<Enum>) readerOpt, e.name());
                        setReaderOption(option);
                        break;
                    }
                }
            }
        } catch (Exception ignored)  /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        if (jReaderObj == null) {
            /* Use normal inputStreamReader if failed to load jline library */
            inputReader = new BufferedReader(new InputStreamReader(input));
        }
    }

    /**
     * Set a variable for the LineReader object by invoking the
     * <code>LineReader.setVariable(String, Object)</code> API
     * @param name variable name
     * @param value variable value
     */
    private void setReaderVariable(String name, Object value) {
        if (jReaderObj != null && setVariableMeth != null) {
            try {
                invokeMethod(jReaderObj, setVariableMeth,
                             new Object[] {name, value});
            } catch (Exception ignored) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Set an option for the LineReader object by invoking the
     * <code>LineReader.setOpt(LineReader.Option)</code> API
     * @param option a LineReader option
     */
    private void setReaderOption(Enum<?> option) {
        if (jReaderObj != null && setOptMeth != null) {
            try {
                invokeMethod(jReaderObj, setOptMeth, new Object[] {option});
            } catch (Exception ignored) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    /**
     * Resume the terminal associated with LineReader by invoking the
     * <code>Terminal.resume()</code> API, which would create a new thread to
     * handle the underlying input streams if the previous one is stopped.
     */
    private void resumeTerminal() {
        if (jTermObj != null && resumeMeth != null) {
            try {
                invokeMethod(jTermObj, resumeMeth, null);
                isTermPaused = false;
            } catch (Exception ignored) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    /* Load command history to shell.CommandHistory. */
    private void loadCommandHistory(Shell shell) {
        if (jFileHistoryObj == null) {
            return;
        }
        final CommandHistory history = shell.getHistory();
        try {
            final int size = getHistorySize();
            if (size == 0) {
                return;
            }
            for (int i = 0; i < size; i++) {
                history.add(getHistoryCommand(i), null);
            }
        } catch (IOException ignored) /* CHECKSTYLE:OFF */  {
            /* Continue if loading command history from history file failed. */
        } /* CHECKSTYLE:ON */
    }

    /* Get number of commands in the history file. */
    private int getHistorySize()
        throws IOException {

        if (jFileHistoryObj != null && sizeMeth != null) {
            return (Integer) invokeMethod(jFileHistoryObj, sizeMeth, null);
        }
        return 0;
    }

    /* Get nth command in the history file. */
    private String getHistoryCommand(int index)
        throws IOException {

        if (jFileHistoryObj != null && getMeth != null) {
            return (String) invokeMethod(jFileHistoryObj, getMeth,
                new Object[] {Integer.valueOf(index)});
        }
        return "";
    }

    /**
     * Return the file for JLine commands history.
     * jline 3 API cannot read history files created by jline2, so we have to
     * use a different naming convention (.jline3-* vs .jline-*) for the
     * history file to avoid conflicts when loading the old history files.
     *
     * If the property is not set, return the default path
     * <user-home>/.jline3-<main-class-name>.history.
     * Return null if the specified file is not readable or writable.
     */
    private static File getHistoryFile(Shell shell) {
        final String path = System.getProperty(PROP_HISTORY_FILE);
        File file = null;
        if (path != null) {
            file = new File(path);
        } else {
            file = new File(System.getProperty("user.home"),
                        ".jline3-" + shell.getClass().getName() + ".history");
        }
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                shell.verboseOutput("Failed to create the command line " +
                    "history file: " + file.getAbsolutePath() +
                    ", command history will be stored in memory.");
                return null;
            }
        } else {
            if (!file.canRead() || !file.canWrite()) {
                shell.verboseOutput("Cannot access the command line " +
                    "history file: " + file.getAbsolutePath() +
                    ", command history will be stored in memory.");
                return null;
            }
        }
        return file;
    }

    private void setHistoryFileSize() {

        final String historySize = System.getProperty(PROP_HISTORY_SIZE);
        if (historySize == null) {
            return;
        }

        int maxSize;
        try {
            maxSize = Integer.valueOf(historySize);
        } catch (NumberFormatException nfe) {
            return;
        }
        if (histFileSizeVar != null) {
            setReaderVariable(histFileSizeVar, maxSize);
        }
    }

    public void shutdown() {
        if (jTermObj != null && closeMeth != null) {
            try {
                invokeMethod(jTermObj, closeMeth, null);
            } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        if (jFileHistoryObj != null && saveMeth != null) {
            try {
                invokeMethod(jFileHistoryObj, saveMeth, null);
            } catch (IOException ignored) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
    }

    private boolean isJlineCompatiblePlatform() {
        /* Check system property that whether jline is disabled. */
        if (Boolean.getBoolean(PROP_JLINE_DISABLE)) {
            return false;
        }

        final String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("windows") != -1) {
            /**
             * Disable jline on Windows because of a Cygwin problem:
             * https://github.com/jline/jline2/issues/62
             * This will be fixed in a later patch.
             */
            return false;
        }
        return true;
    }

    public void setDefaultPrompt(String prompt) {
        this.prompt = prompt;
    }

    public String getDefaultPrompt() {
        return this.prompt;
    }

    public String readLine()
        throws IOException {

        return readLine(null);
    }

    public String readLine(String promptString)
        throws IOException {
        final String promptStr = (promptString != null) ?
            promptString : this.prompt;
        if (isTermPaused) {
            resumeTerminal();
        }
        if (jReaderObj != null && readLineMeth != null) {
            return (String) invokeMethod(jReaderObj, readLineMeth,
                                        new Object[]{promptStr});
        }
        if (promptStr != null) {
            output.print(promptStr);
        }
        return inputReader.readLine();
    }

    public char[] readPassword(String promptString) throws IOException {
        String input = null;
        final String pwdPrompt = (promptString != null) ? promptString :
                                                          this.prompt;
        if (isTermPaused) {
            resumeTerminal();
        }
        if (jReaderObj != null && readLineWithMaskMeth != null) {
            input = (String) invokeMethod(jReaderObj, readLineWithMaskMeth,
                new Object[]{pwdPrompt, Character.valueOf((char) 0)});
            return input == null ? null : input.toCharArray();
        }

        if (!Boolean.getBoolean(PROP_SYS_CONSOLE_DISABLE)) {
            final Console console = System.console();
            if (console != null) {
                return console.readPassword(pwdPrompt);
            }
        }

        output.print(pwdPrompt);
        input = inputReader.readLine();
        return input == null ? null : input.toCharArray();
    }

    public int getTerminalHeight() {
        if (jTermObj != null && getHeightMeth != null) {
            try {
                return (Integer) invokeMethod(jTermObj, getHeightMeth, null);
            } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return getTermHeightImpl();
    }

    private Object invokeMethod(Object obj, Method method, Object[] args)
        throws IOException {

        final String name = method.getName();
        try {
            return method.invoke(obj, args);
        } catch (IllegalAccessException | IllegalArgumentException ex) {
            final String clsName = method.getDeclaringClass().getSimpleName();
            final String msg =
                String.format("Failed to invoke %s.%s.", clsName, name);
            throw new IOException(msg, ex);
        } catch (InvocationTargetException ite) {
            final Throwable cause = ite.getCause();
            if (cause == null) {
                throw new IOException(ite);
            }
            if (obj == jReaderObj && "readLine".equals(name)) {
                if (cause instanceof IOError) {
                    /**
                     * As of jline-3.7.1, if terminal input reader runs into an
                     * IOException, it stops reading and wraps the IOException
                     * in an IOError, which is conveyed back to users over the
                     * LineReader.readLine() API. We need to resume the
                     * terminal in the next read attempt.
                     */
                    isTermPaused = true;
                } else if (userInterruptException.isInstance(cause) ||
                           endOfFileException.isInstance(cause)) {
                    return null;
                }
            }
            throw new IOException(cause.getMessage(), cause);
        }
    }

    private int getTermHeightImpl() {
        final String os = System.getProperty("os.name").toLowerCase();
        if (os.indexOf("windows") != -1) {
            return TERMINAL_HEIGHT_DEFAULT;
        }
        int height = getUnixTermHeight();
        if (height == -1) {
            height = TERMINAL_HEIGHT_DEFAULT;
        }
        return height;
    }

    /*
     * stty -a
     *  speed 38400 baud; rows 48; columns 165; line = 0; ...
     */
    private int getUnixTermHeight() {
        String ttyProps = null;
        final String name = "rows";
        try {
            ttyProps = getTermSttyProps();
            if (ttyProps != null && ttyProps.length() > 0) {
                return getTermSttyPropValue(ttyProps, name);
            }
        } catch (IOException ignored)  /* CHECKSTYLE:OFF */ {
        } catch (InterruptedException ignored) {
        } /* CHECKSTYLE:ON */
        return -1;
    }

    private String getTermSttyProps()
        throws IOException, InterruptedException {

        final String[] cmd = {"/bin/sh", "-c", "stty -a </dev/tty"};
        final Process proc = Runtime.getRuntime().exec(cmd);

        String s = null;
        final StringBuilder sb = new StringBuilder();
        final BufferedReader stdInput =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));
        while ((s = stdInput.readLine()) != null) {
            sb.append(s);
        }

        final BufferedReader stdError =
            new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        while ((s = stdError.readLine()) != null) {
            sb.append(s);
        }

        proc.waitFor();
        return sb.toString();
    }

    private int getTermSttyPropValue(String props, String name) {
        final StringTokenizer tokenizer = new StringTokenizer(props, ";\n");
        while (tokenizer.hasMoreTokens()) {
            final String str = tokenizer.nextToken().trim();
            if (str.startsWith(name)) {
                return Integer.parseInt(
                        str.substring(name.length() + 1, str.length()));
            } else if (str.endsWith(name)) {
                return Integer.parseInt(
                        str.substring(0, (str.length() - name.length() - 1)));
            }
        }
        return 0;
    }

}
