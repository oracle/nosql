/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Stack;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import oracle.nosql.common.json.JsonUtils;

/**
 * ClassChangeDetector is a tool to compare two jar files representing
 * different versions of the same product, to detect incompatibilities that
 * might arise during upgrades.  The two jar files are specified by the
 * required -oldJar and -newJar command arguments.
 * <p>
 * The tool can perform two main types of analysis.  One type looks at all
 * remote interfaces and their arguments, to check for protocol
 * differences.  This mode of operation is requested with the
 * -checkProtocols command argument.
 * <p>
 * The other useful type of analysis is to check for differences in database
 * schema: i.e. classes that are stored persistently in BDB JE databases.
 * This mode is requested with the -checkDbSchema command argument.
 * <p>
 * The tool can store detected changes in a JSON-formatted file, given by
 * the -changesFile argument.  Each change is represented as a stanza that is
 * the serialized form of the Change class.  Each change has a boolean field
 * named "ok".  The value given to an "ok" field indicates whether a change has
 * been examined, possibly tested for upgrade compatibility, and declared to be
 * an accepted change.  These values can be altered in the JSON file with a
 * text editor.  Subsequent runs of ClassChangeDetector, given the same
 * -changesFile argument, will read the file and note which changes are
 * accepted.  It will suppress reporting of accepted changes unless the
 * -showAccepted argument is given.
 * <p>
 * If a changesFile argument is given, it is always written back with its
 * original contents intact, along with any new changes that were detected.
 * <p>
 * The two main jar files to be compared, and the classes they contain, are
 * termed "primary".  The tool examines each primary jar's manifest for a
 * classpath, and seeks to include every jar mentioned there.  These are termed
 * "stipulated" jars.  If a stipulated jar can't be found, a warning is issued,
 * but the tool will try to continue.  If a jar file that is not stipulated is
 * needed for the analysis , it can be given with the -extraJar argument, which
 * can be repeated as many times as is necessary.  A different set of
 * stipulated jars can be associated with each primary jar, but the set of
 * extraJars is shared between the two primaries.  This may turn out to be a
 * weakness that will need to be addressed down the road.
 */
public class ClassChangeDetector {

    final static String usageString =
        "Usage: ...ClassChangeDetector " +
        "-oldJar <jarfile> -newJar <jarfile> " +
        "{-checkDbSchema | -checkProtocols} " +
        "[-changesFile <file>] " +
        "[-extraJar <jarfile>]..." +
        "[-ignore <prefix>]..." +
        "[-explain <class>]";

    final Set<String> seenClasses = new HashSet<String>();
    Changes changes = new Changes();
    File changesFile = null;
    int verbose = 0;
    boolean followFields = true;
    boolean examineTransientFields = true;
    boolean examineStaticFields = true;
    boolean ignoreUnserializableClasses = false;
    boolean onlySchema = false;
    boolean onlyProtocol = false;
    boolean phase1isOver = false;

    String jarPath1;
    String jarPath2;

    List<String> extraJars = new ArrayList<String>();
    List<String> ignorablePrefixes = new ArrayList<String>();

    static Stack<String> breadCrumbs = new Stack<String>(); /* used in Change ctor */
    String explainClass = null;  /* Explain why this class is included */

    CCDClassLoader classLoader1;
    CCDClassLoader classLoader2;

    private ClassChangeDetector() {
    }

    private static void printUsage(String msg) {
        if (msg != null) {
            System.err.println(msg);
        }
        System.err.println(usageString);
        System.exit(-1);
    }

    private void parseArgs(String[] argv) {
        int argc = 0;
        int nArgs = argv.length;

        if (nArgs == 0) {
            printUsage(null);
        }

        /*
         * Not using kv's CommandParser because this might become an
         * independent tool.
         */
        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if ("-oldJar".equals(thisArg)) {
                if (argc < nArgs) {
                    jarPath1 = argv[argc++];
                } else {
                    printUsage("-oldJar requires a jar filename argument.");
                }
            } else if ("-newJar".equals(thisArg)) {
                if (argc < nArgs) {
                    jarPath2 = argv[argc++];
                } else {
                    printUsage("-newJar requires a jar filename argument.");
                }
            } else if ("-checkDbSchema".equals(thisArg)) {
                onlySchema = true;
            } else if ("-checkProtocols".equals(thisArg)) {
                onlyProtocol = true;
            } else if ("-extraJar".equals(thisArg)) {
                if (argc < nArgs) {
                    extraJars.add(argv[argc++]);
                } else {
                    printUsage
                        ("-extraJar requires a jar pathname argument");
                }
            } else if ("-ignore".equals(thisArg)) {
                if (argc < nArgs) {
                    ignorablePrefixes.add(argv[argc++]);
                } else {
                    printUsage
                        ("-ignore requires a package name argument");
                }
            } else if ("-explain".equals(thisArg)) {
                if (argc < nArgs) {
                    explainClass = argv[argc++];
                } else {
                    printUsage
                        ("-explain requires a class name argument");
                }
            } else if ("-v".equals(thisArg)) {
                verbose++;
            } else if ("-changesFile".equals(thisArg)) {
                if (argc < nArgs) {
                    changesFile = new File(argv[argc++]);
                } else {
                    printUsage
                        ("-changesFile requires a changes file path argument");
                }
            } else {
                printUsage("Unrecognized option argument " + thisArg);
            }
        }

        if (jarPath1 == null || jarPath2 == null) {
            printUsage("Both -oldJar and -newJar options are required");
        }

        if (onlySchema && onlyProtocol) {
            printUsage
               ("Only one of -checkDbSchema and -checkProtocols can be given.");
        }

        /* Add standard ignorable prefix to the provided list. */
        ignorablePrefixes.add("java.");
    }

    /**
     * In schema mode, we'll look only at classes that are annotated
     * as @Persistent or @Entity.  There is no need to follow fields to find
     * more classes to analyze, because all classes referred to by
     * non-transient instance fields in persistent classes should themselves be
     * marked as persistent.  We ignore transients and statics in this mode.
     */
    private void setSchemaMode() {
        followFields = false;
        examineTransientFields = false;
        examineStaticFields = false;
        ignoreUnserializableClasses = false;
    }

    /**
     * In protocol mode, we want to examine the method signatures of remote
     * interfaces, and all of the arguments to these methods.  If an argument
     * to a remote method is itself a remote interface, we do not examine its
     * children.  Transients and statics are ignored.
     */
    private void setProtocolMode() {
        followFields = true;
        examineTransientFields = false;
        examineStaticFields = false;
        ignoreUnserializableClasses = true;
    }

    /**
     * In primary mode, we look at every primary class and its fields.  A
     * primary class is one that resides in the target jar files, and not in
     * any stipulated or extra jars.  This mode is not useful for testing
     * compatibility, but it does produce an interesting report.
     */
    @SuppressWarnings("unused")
    private void setPrimaryMode() {
        followFields = true;
        examineTransientFields = true;
        examineStaticFields = true;
    }

    private void initialize() {
        classLoader1 = new CCDClassLoader(jarPath1);
        classLoader2 = new CCDClassLoader(jarPath2);
    }

    public static void main(String[] args) {

        ClassChangeDetector ccd = new ClassChangeDetector();

        ccd.parseArgs(args);

        ccd.initialize();

        /*
         * At this point, we should have every class that we'll need.  Any
         * attempt to instantiate a class after this is considered an error.
         */
        ccd.phase1isOver = true;

        if (ccd.onlySchema) {
            ccd.checkSchema();
        } else if (ccd.onlyProtocol) {
            ccd.checkProtocol();
        } else {
            printUsage
                ("One of [-checkDbSchema] and [-checkProtocols] is needed.");
        }

        if (ccd.changesFile != null) {
            boolean existed = false;
            if (ccd.changesFile.exists()) {
                File backupFile = new File(ccd.changesFile.getPath() + ".bak");
                ccd.changesFile.renameTo(backupFile);
                existed = true;
            }
            try {
                System.err.println((existed ? "Overwriting " : "Creating ") +
                                   ccd.changesFile.toString());
                saveChanges(ccd.changesFile, ccd.changes);
            } catch (Exception e) {
                printUsage("There was a problem writing the Changes file " +
                           ccd.changesFile.toString() + ": " +
                           e.getMessage());
            }
        }
    }

    private void checkSchema() {
        setSchemaMode();

        Set<String> classesToCheck =
            classLoader1.getAllPersistentClassNames();
        Set<String> classesInNewJar =
            classLoader2.getAllPersistentClassNames();

        /* First check these two sets for congruence or lack thereof */
        final Set<String> onlyin1 = new HashSet<String>(classesToCheck);
        onlyin1.removeAll(classesInNewJar);
        final Set<String> onlyin2 = new HashSet<String>(classesInNewJar);
        onlyin2.removeAll(classesToCheck);

        if (onlyin1.size() > 0) {
            tell(1, "These classes are NO LONGER PERSISTENT:");
            for (String s : onlyin1) {
                changes.recordClassNoLongerPersistent
                    (s, breadCrumbs.toArray(new String[] {}));
                tell(1, "    " + s);
            }
        }

        if (onlyin2.size() > 0) {
            tell(1, "These classes are NEWLY PERSISTENT:");
            for (String s : onlyin2) {
                changes.recordClassNewlyPersistent
                    (s, breadCrumbs.toArray(new String[] {}));
                tell(1, "    " + s);
            }
        }

        /* Now compare the classes that exist in both. */
        classesToCheck.removeAll(onlyin1);

        for (String s : classesToCheck) {
            tell(2, "Checking " + s);
            reachable(s, false);
        }
    }

    private void checkProtocol() {
        setProtocolMode();

        Set<String> remoteInterfacesToCheck =
            classLoader1.getAllRemoteInterfaceNames();
        Set<String> remoteInterfacesInNewJar =
            classLoader2.getAllRemoteInterfaceNames();

        /* First check these two sets for congruence or lack thereof */
        final Set<String> onlyin1 =
            new HashSet<String>(remoteInterfacesToCheck);
        onlyin1.removeAll(remoteInterfacesInNewJar);
        final Set<String> onlyin2 =
            new HashSet<String>(remoteInterfacesInNewJar);
        onlyin2.removeAll(remoteInterfacesToCheck);

        if (onlyin1.size() > 0) {
            tell(1, "These remote interfaces have been REMOVED:");
            for (String s : onlyin1) {
                changes.recordRemoteInterfaceRemoved
                    (s, breadCrumbs.toArray(new String[] {}));
                tell(1, "    " + s);
            }
        }

        if (onlyin2.size() > 0) {
            tell(1, "These remote interfaces have been added:");
            for (String s : onlyin2) {
                changes.recordRemoteInterfaceAdded
                    (s, breadCrumbs.toArray(new String[] {}));
                tell(1, "    " + s);
            }
        }

        /* Now compare the interfaces that exist in both. */

        remoteInterfacesToCheck.removeAll(onlyin1);

        for (String rmi : remoteInterfacesToCheck) {

            tell(2, "Checking " + rmi);

            final ClassEntry ce1 =
                classLoader1.getClassEntryByName(rmi);
            if (ce1 == null) {
                throw new RuntimeException
                    ("Can't find " + rmi + " in " + jarPath1);
            }
            final ClassEntry ce2 =
                classLoader2.getClassEntryByName(rmi);
            if (ce2 == null) {
                throw new RuntimeException
                    ("Can't find " + rmi + " in " + jarPath2);
            }

            compareMethods(ce1, ce2);

            Set<String> classesToCheck =
                classLoader1.getArgTypesForMethods(rmi,
                                                   true); /* ignore static. */

            for (String c : classesToCheck) {
                reachable(c, false);
            }
        }
    }

    private void reachable(String className,
                           boolean onlyEnums) {

        breadCrumbs.push(className);

        try {
            explainMaybe();

            if (seenClasses.contains(className)) {
                return;
            }

            final ClassEntry ce1 = classLoader1.getClassEntryByName(className);
            if (ce1 == null) {
                /* We don't really care about analyzing this type. */
                return;
            }

            /*
             * When reachable is called from examineFields, sometimes we want to
             * examine the class only if it is not an enum type.  See the comment
             * at that call site for further explanation.
             */
            if (onlyEnums && ! ce1.isEnum()) {
                return;
            }

            /*
             * Add to the "seen" list AFTER checking onlyEnums.  We might get
             * called with onlyEnums == false later, in which case we'll want to
             * examine it then.
             */
            seenClasses.add(className);

            if (isIgnorableClassName(className)) {
                return;
            }

            tell(3, "Reachable for " + className);

            final ClassEntry ce2 = classLoader2.getClassEntryByName(className);
            if (ce2 == null) {
                /*
                 * This can happen if a class has been removed in jar2.  The
                 * removal or changing of any field that referred to this
                 * now-missing class should be detected by compareFields, and
                 * should be reported as such.  Therefore we can forgo pursuing
                 * this line of inquiry.
                 */
                return;
            }

            /*
             * Examine extenders and implementors of this class, unless we are
             * looking only at protocols, and this is a remote interface.  We
             * aren't interested in the implementors of remote interfaces when
             * analyzing protocols; we are interested only in the interfaces'
             * method signatures and their arguments.  We might reach this
             * point with a remote interface by following another interface's
             * method arguments.  If an argument to a remote method is itself a
             * remote interface, it should be checked separately AS a remote
             * interface, and not as a serializable argument.  Do this now,
             * before checking for ignoreSerializableClasses, because even if
             * this class or interface is not serializable, an extender or
             * implementor might be.
             */
            if (ce1.isRemoteInterface() && onlyProtocol)  {
                return;
            }
            followChildren(ce1);

            if (ignoreUnserializableClasses && !(ce1.isSerializable() ||
                                                 ce2.isSerializable())) {
                return;
            }

            checkInheritance(ce1, ce2);

            /*
             * Compare the classes' field sets, also following fields that
             * refer to classes.
             */
            examineFields(ce1, ce2);

        } finally {
            breadCrumbs.pop();
        }
    }

    private void explainMaybe() {
    	String className = breadCrumbs.peek();
        if (className.equals(explainClass)) {
            int indent = 0;
            for (String s : breadCrumbs) {
                for (int i = 0; i < indent; i++) {
                    System.err.print(" ");
                }
                System.err.println(s);
                indent++;
            }
        }
    }

    /**
     * Compare the classes' inheritance chains, and note discrepancies.
     */
    private void checkInheritance(ClassEntry ce1, ClassEntry ce2) {

        Class<?> c1 = ce1.getClazz();
        if (c1 == null) {
            return;
        }
        Class<?> c2 = ce2.getClazz();
        if (c2 == null) {
            return;
        }

        List<String> chain1 = new ArrayList<String>();
        List<String> chain2 = new ArrayList<String>();
        if (checkInheritance(c1, c2, chain1, chain2)) {
            /* No need to mention the class we are checking. */
            if (chain1.size() > 1) {
                chain1.remove(0);
            }
            if (chain2.size() > 1) {
                chain2.remove(0);
            }
            changes.recordClassInheritanceChanged
                (c1.getName(),
                 breadCrumbs.toArray(new String[] {}),
                 joinStringList(chain1, "->"),
                 joinStringList(chain2, "->"));
        }
    }

    /**
     * Recurse up the inheritance chain.  Return value of "true" means that a
     * change was noticed.
     */
    @SuppressWarnings("null")
    private boolean checkInheritance(Class<?> c1, Class<?> c2,
                                     List<String> chain1, List<String>chain2) {

        if (c1 == null && c2 == null) {
            return false;
        }

        if (c1 != null) {
            chain1.add(c1.getName());
            if (c2 == null) {
                return true;
            }
        }

        if (c2 != null) {
            chain2.add(c2.getName());
            if (c1 == null) {
                return true;
            }
        }

        /*
         * Neither can be null here, but Eclipse disagrees.
         * Hence the @SuppressWarnings, above.
         */
        if (! c1.getName().equals(c2.getName())) {
            return true;
        }

        Class<?> p1 = c1.getSuperclass();
        Class<?> p2 = c2.getSuperclass();

        return checkInheritance(p1, p2, chain1, chain2);
    }

    /* Merge List of String into one String, with delimiter. */
    private static String joinStringList(List<String> a, String delimiter) {
        String r = "";
        if (a != null) {
            int n = 0;
            for (String s : a) {
                if (n++ > 0) {
                    r += delimiter;
                }
                r += s;
            }
        }
        return r;
    }

    /*
     * Compare the fields belonging to the two classes.  If followFields is
     * true, then also chase down and compare classes referenced by the fields.
     */
    private void examineFields(ClassEntry ce1, ClassEntry ce2) {
        /*
         * If the class information is missing, we can't follow the fields.
         * This could happen if the entry is for an external class that is
         * extended by one of our classes of interest.
         */
        if (ce1.getClazz() == null) {
            return;
        }

        compareFields(ce1, ce2);

        Set<String> classesToCheck =
            classLoader1.getTypesForAllFields(ce1.getName(),
                                              !examineStaticFields);
        breadCrumbs.push("has field of type");
        for (String c : classesToCheck) {
            /*
             * If followFields is false, then the set of classes to be checked
             * has been determined by other means, and we don't want to chase
             * field references to find more types to check.  This is the case
             * when checking schema: we'll be looking at every class annotated
             * with @Entity or @Persistent, and no others -- UNLESS the class
             * is an Enum type.  Enum values are stored persistently, but are
             * not marked as such.  It is especially critical to vet changes in
             * enum value sets.  Enum value sets can be changed only by having
             * new values added at the end of the list.  Values should not be
             * renamed or deleted.
             */
            reachable(c, !followFields);
        }
        breadCrumbs.pop();
    }

    private void followChildren(ClassEntry ce1) {
        breadCrumbs.push("has child of type");
        for (ClassEntry ce : ce1.getChildren()) {
            reachable(ce.getName(), false);
        }
        breadCrumbs.pop();
    }

    /* The default list of children for classes with no children. */
    static private final List<ClassEntry> emptyChildrenList =
        Collections.emptyList();

    /**
     * We need to be able to navigate from "parent" classes and interfaces to
     * their children.  This information is not available in the classes
     * themselves, so we assemble it by wrapping each Class in ClassEntry,
     * which contains references to the children of the Class.
     */
    private class ClassEntry {
        Class<?> clazz;
        List<ClassEntry> children = null;
        boolean primary = false; /* True if clazz belongs to the subject jar. */

        private ClassEntry() {
        }

        private void setClass(Class<?> c) {
            this.clazz = c;
        }

        private Class<?> getClazz() {
            return this.clazz;
        }

        private void addChild(ClassEntry ce) {
            if (children == null) {
                children = new ArrayList<ClassEntry>();
            }
            children.add(ce);
        }

        private List<ClassEntry> getChildren() {
            return children == null ? emptyChildrenList : children;
        }

        private String getName() {
            return clazz == null ? null : clazz.getName();
        }

        private boolean isEnum() {
            return clazz == null ? false : clazz.isEnum();
        }

        private void markPrimary() {
            primary = true;
        }

        /**
         *  Tell whether the class is part of the subject jar file.
         */
        private boolean isPrimary() {
            return primary;
        }

        /**
         * Tell whether the class is part of a database schema.
         */
        private boolean isPersistent() {

            if (clazz == null) {
                return false;
            }

            Annotation[] annotations = clazz.getAnnotations();
            for (Annotation a : annotations) {
                if ((a.annotationType().getName().equals
                     ("com.sleepycat.persist.model.Entity")) ||
                    (a.annotationType().getName().equals
                     ("com.sleepycat.persist.model.Persistent"))) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Tell whether this class is a remote interface.
         */
        private boolean isRemoteInterface() {

            if (clazz == null) {
                return false;
            }

            if (clazz.isInterface() &&
                java.rmi.Remote.class.isAssignableFrom(clazz)) {

                return true;
            }
            return false;
        }

        /**
         * Tell whether this class implements
         * Serializable or FastExternalizable.
         */
        private boolean isSerializable() {

            if (clazz == null) {
                return false;
            }

            boolean v = java.io.Serializable.class.isAssignableFrom(clazz);
            if (!v) {
                try {
                    v = clazz.getClassLoader().
                        loadClass("oracle.kv.impl.util.FastExternalizable").
                        isAssignableFrom(clazz);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException
                        ("Can't find FastExternalizable!");
                }
            }

            tell(5, "isSerializable: " + clazz.getName() + ":" + v);

            return v;
        }

        /**
         * Return a list of all fields, including both declared fields and
         * inherited fields.
         */
        private List<FieldWrapper> getAllFields() {
            final List<FieldWrapper> fws = new ArrayList<FieldWrapper>();

            /* Walk up the class hierarchy, collecting all declared fields. */
            Class<?> c = getClazz();
            while (c != null && c != Object.class) {
                final Field[] fields = c.getDeclaredFields();
                for(Field f : fields) {
                    if (isIgnorableField(f)) {
                        continue;
                    }
                    fws.add
                        (new FieldWrapper
                         (f, breadCrumbs.toArray(new String[] {})));
                }
                c = c.getSuperclass();
            }
            Collections.sort(fws);
            return fws;
        }

        public List<MethodWrapper> getAllMethods() {
            final List<MethodWrapper> mws = new ArrayList<MethodWrapper>();

            /* Walk up the class hierarchy, collecting all declared methods. */
            Class<?> c = getClazz();
            while (c != null && c != Object.class) {
                final Method[] methods = c.getDeclaredMethods();
                for (Method m : methods) {
                    mws.add
                        (new MethodWrapper
                         (m, breadCrumbs.toArray(new String[] {})));
                }
                c = c.getSuperclass();
            }
            Collections.sort(mws);
            return mws;
        }
    }

    private class CCDClassLoader extends ClassLoader {
        private JarFile subjectJar;
        private List<JarFile> stipulatedJars;/* Jars on which subject depends */
        private final Map<String,ClassEntry> classEntries =
            new HashMap<String,ClassEntry>();

        /**
         * Upon construction of the classloader, we load ALL the classes so
         * that we can find the parent-child relationships.
         */
        private CCDClassLoader(String jarPath) {
            super(null);
            try {
                subjectJar = new JarFile(jarPath);
            } catch (IOException ioe) {
                printUsage("Can't open " + jarPath + ": " + ioe.getMessage());
            }

            setStipulatedJars();

            Enumeration<JarEntry> entries = subjectJar.entries();
            while(entries.hasMoreElements()) {
                JarEntry cje = entries.nextElement();
                String jeName = cje.getName();
                if (jeName.endsWith(".class") &&
                    ! jeName.contains("WEB-INF")) {
                    String className =
                        jeName.replace('/', '.').replaceAll("\\.class$", "");
                    if (isIgnorableClassName(className)) {
                        tell(3, "Ignoring " + className);
                        continue;
                    }
                    tell(3, "Discovered " + className);
                    ClassEntry ce = getOrCreateClassEntry(className);
                    if (ce.getClazz() == null) {
                        try {
                            defineClassEntry(className, cje, subjectJar);
                        } catch (IOException | NoClassDefFoundError e) {
                            printUsage
                                ("Can't define class " + className +
                                 " because of failure to locate " + e.getMessage());
                        }
                    }
                }
            }

            /*
             * Pull in classes that will be referenced later when we examine
             * the fields and annotations of these classes.
             */
            while (true) {
                try {
                    for (ClassEntry ce : classEntries.values()) {
                        Class<?> c = ce.getClazz();
                        if (c != null && !isIgnorableClassName(c.getName())) {
                            try {
                                c.getDeclaredFields();
                                c.getAnnotations();
                                c.getTypeParameters();
                                for (Field f : c.getDeclaredFields()) {
                                    f.getGenericType();
                                }
                            } catch (NoClassDefFoundError e) {
                                printUsage
                                ("Can't define class " + c.getName() +
                                 " because of failure to locate " + e.getMessage());

                            }
                        }
                    }
                } catch (ConcurrentModificationException cme) {
                    continue; /* Start over; there is a new class to look at. */
                }
                break;
            }
        }

        /**
         * Derive the complete set of jars needed to satisfy references from
         * the subject jar.  With kvstore.jar, we can assume that the list is
         * available in the manifest.  We can augment the list from the
         * command line as well.
         */
        private void setStipulatedJars() {

            File subjectPath = new File(subjectJar.getName());
            File subjectDir = subjectPath.getParentFile();

            String manifestClassPath = null;
            try {
                manifestClassPath = subjectJar.getManifest().
                    getMainAttributes().getValue(Attributes.Name.CLASS_PATH);
            } catch (IOException ioe) {
                System.err.println
                    ("Warning: can't read subject jar's manifest. " +
                     ioe.getMessage());
            }
            if (manifestClassPath != null) {
                String[] stipulatedJarNames = manifestClassPath.split("\\s+");
                stipulatedJars = new ArrayList<JarFile>();

                for (String s : stipulatedJarNames) {
                    try {
                        stipulatedJars.add
                            (new JarFile(new File(subjectDir, s)));
                    } catch (IOException ze) {
                        /* Couldn't open one of the jars, just skip it. */
                        System.err.println
                            ("Warning: Can't open " + s +
                             ", which is stipulated " +
                             "in " + subjectJar.getName() + "'s manifest.");
                    }
                }
            }

            /* Add the extra jars given on the command line. */
            for (String s : extraJars) {
                try {
                    stipulatedJars.add(new JarFile(new File(s)));
                } catch (IOException ioe) {
                    printUsage
                        ("Problem opening " + s + ": " + ioe.getMessage());
                }
            }
        }

        private ClassEntry defineClassEntry(String className,
                                            JarEntry cje,
                                            JarFile jf)
            throws IOException {

            byte[] data = new byte[(int)cje.getSize()];
            InputStream is = jf.getInputStream(cje);

            try {
                /*
                 * Read the jar entry into the byte array.  If the entry is
                 * large, we might go around the loop a few times.
                 */
                int off = 0, nr = 0;
                while (off < data.length) {
                    nr = is.read(data, off, data.length - off);
                    if (nr < 0) { /* EOF */
                        break;
                    }
                    off += nr;
                }

                if (off < data.length) {
                    throw new IOException
                        ("Short read on " + className + "'s jar entry.");
                }
            } finally {
                is.close();
            }

            Class<?> newClass =
                defineClass(className, data, 0, data.length);

            ClassEntry ce = updateClassEntry(newClass);

            Class<?>[] interfaces = newClass.getInterfaces();
            for (Class<?> c : interfaces) {
                noteParent(ce, c);
            }
            Class<?> superClass = newClass.getSuperclass();
            if (superClass != null) {
                noteParent(ce, superClass);
            }

            /* Mark "primary" classes--those that belong to the subject jar. */
            if (jf == subjectJar) {
                ce.markPrimary();
            }

            return ce;
        }

        @Override
        public Class<?> findClass(String className)
            throws ClassNotFoundException {

            tell(4, "findClass called for " + className);
            ClassEntry ce = getOrCreateClassEntry(className);
            if (ce.getClazz() == null) {

                String jeName = className.replace('.', '/') + ".class";

                /* Look first in the subject jar file. */
                JarFile inJar = null;
                ZipEntry zje = subjectJar.getEntry(jeName);
                if (zje != null) {
                    inJar = subjectJar;
                } else {
                    /*
                     * The class wasn't in the subject, so check for it in the
                     * stipulated jars.
                     */
                    for (JarFile jf : stipulatedJars) {
                        zje = jf.getEntry(jeName);
                        if (zje != null) {
                            inJar = jf;
                            break;
                        }
                    }
                }

                if (zje == null) {
                    if (isIgnorableClassName(className)) {
                        return fakeClass(className);
                    }
                    throw new ClassNotFoundException
                        ("Can't find the class " + jeName);
                }

                try {
                    ce = defineClassEntry(className, new JarEntry(zje), inJar);
                } catch (IOException e) {
                    throw new ClassNotFoundException
                        ("troubled defining class", e);
                }
            }

            return ce.getClazz();
        }

        /*
         * Synthesize a fake class to substitute for ignorable classes
         * encountered along the way.  These synthetic classes are just
         * dummies to satisfy references, and do not contain any references
         * themselves..
         */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        private Class<?> fakeClass(final String className) {
            tell(4, "Making fake class " + className);
            int i = className.lastIndexOf(".");
            final String simpleName =  className.substring(i + 1);
            final String packageName = className.substring(0, i);
            final String pathName = className.replace('.', '/');

            /* Write the fake class's source code into a String. */

            final String src =
                String.format("package %s;\n" +
                              "public class %s {\n" +
                              "  public String toString() {\n" +
                              "    return \"synthetic class for %s.%s\";\n" +
                              "  }\n" +
                              "}\n",
                              packageName,
                              simpleName,
                              packageName,
                              simpleName);

            /* Set up an outputstream to receive the class bytes. */
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final SimpleJavaFileObject sjfo =
                new SimpleJavaFileObject(URI.create(pathName + ".java"),
                                         JavaFileObject.Kind.SOURCE) {

                    @Override
                    public CharSequence getCharContent(boolean ignore) {
                        return src;
                    }

                    @Override
                    public OutputStream openOutputStream() throws IOException {
                        return baos;
                    }
                };

            final JavaFileManager jfm = new ForwardingJavaFileManager
                    (ToolProvider.getSystemJavaCompiler().
                     getStandardFileManager(null, null, null)) {

                    @Override
                    public JavaFileObject getJavaFileForOutput
                        (final Location location,final String cName,
                         final JavaFileObject.Kind kind,
                         final FileObject sibling) {

                        return sjfo;
                    }
                };

            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            if (compiler == null) {
                throw new RuntimeException("Can't get compiler object." +
                                           "Please run with a Sun JDK");
            }

            compiler.getTask
                (null, jfm, null, null, null, Collections.singletonList(sjfo))
                .call();

            /* The outputstream will have collected the compiled class. */

            final byte[] classbytes = baos.toByteArray();

            return defineClass(className, classbytes, 0, classbytes.length);
        }

        private ClassEntry updateClassEntry(Class<?> clazz) {
            String className  = clazz.getName();
            ClassEntry ce = getOrCreateClassEntry(className);
            ce.setClass(clazz);
            return ce;
        }

        private void noteParent(ClassEntry child, Class<?> parent) {
            String parentClassName = parent.getName();
            ClassEntry ce = getOrCreateClassEntry(parentClassName);
            ce.addChild(child);
        }

        /**
         * During phase1, getOrCreateClassEntry will install a new entry if the
         * class is not found.  After phase1 is over, seeking a class that is
         * not present is considered an error, unless it is an ignorable class,
         * in which case we make a fake class to represent it.
         */
        private ClassEntry getOrCreateClassEntry(String className) {
            ClassEntry ce = classEntries.get(className);
            if (ce == null) {
                final boolean ignorable = isIgnorableClassName(className);
                if (phase1isOver && !ignorable) {
                    throw new RuntimeException("Attempt to load class " +
                                               className + " after phase1");
                }
                ce = new ClassEntry();
                classEntries.put(className, ce);
                if (phase1isOver && ignorable) {
                    ce.setClass(fakeClass(className));
                }
            }
            return ce;
        }

        private ClassEntry getClassEntryByName(String className) {
            return classEntries.get(className);
        }

        @SuppressWarnings("unused")
        private Set<String> getAllPrimaryClassNames() {
            final Set<String> primaries = new HashSet<String>();

            for (ClassEntry ce : classEntries.values()) {
                if (ce.isPrimary() && !isIgnorableClassName(ce.getName())) {
                    primaries.add(ce.getName());
                }
            }
            return primaries;
        }

        private Set<String> getAllPersistentClassNames() {
            final Set<String> persistents = new HashSet<String>();

            for (ClassEntry ce : classEntries.values()) {
                if (ce.isPrimary() && ce.isPersistent()) {
                    persistents.add(ce.getName());
                }
            }
            return persistents;
        }

        @SuppressWarnings("unused")
        private Set<String> getAllSerializableClassNames() {
            final Set<String> serializables = new HashSet<String>();

            for (ClassEntry ce : classEntries.values()) {
                if (ce.isSerializable()) {
                    serializables.add(ce.getName());
                }
            }
            return serializables;
        }

        private Set<String> getAllRemoteInterfaceNames() {
            final Set<String> remotes = new HashSet<String>();

            for(ClassEntry ce : classEntries.values()) {
                if (ce.isPrimary() && ce.isRemoteInterface()) {
                    remotes.add(ce.getName());
                }
            }
            return remotes;
        }

        /**
         * Return a set of the names of all types used by methods in the given
         * class.  Parameterized types are taken apart and each type involved
         * is included separately.
         */
        private Set<String> getArgTypesForMethods(String className,
                                                  boolean ignoreStatic) {

            final Set<String> types = new HashSet<String>();

            final ClassEntry ce = classEntries.get(className);
            if (ce == null) {
                return types;
            }

            Class<?> clazz = ce.getClazz();
            if (clazz == null) {
                return types;
            }

            /* Iterate over all the methods. */
            for (MethodWrapper mw : ce.getAllMethods()) {
                if (ignoreStatic && mw.isStatic()) {
                    continue;
                }

                explodeType(mw.getGenericReturnType(), types);

                for (Type argType : mw.getGenericParameterTypes()) {
                    explodeType(argType, types);
                }
            }

            return types;
        }

        /**
         * Return a set of the names of all types used in fields of the given
         * class.  Parameterized types are taken apart and each type involved
         * is included separately.
         */
        private Set<String> getTypesForAllFields(String className,
                                                 boolean ignoreStatic) {

            final Set<String> types = new HashSet<String>();

            final ClassEntry ce = classEntries.get(className);
            if (ce == null) {
                return types;
            }

            Class<?> clazz = ce.getClazz();
            if (clazz == null) {
                return types;
            }

            /* Iterate over all the fields. */
            for (FieldWrapper fw : ce.getAllFields()) {
                if (ignoreStatic && fw.isStatic()) {
                    continue;
                }
                explodeType(fw.getGenericType(), types);
            }
            return types;
        }

        /**
         * Recursively take apart generic types, adding actual class names to
         * the given set as they are found.
         */
        void explodeType(Type given, Set<String> classNames) {
            if (given instanceof ParameterizedType) {
                ParameterizedType p = (ParameterizedType)given;
                explodeType(p.getRawType(), classNames);
                for (Type t : p.getActualTypeArguments()) {
                    explodeType(t, classNames);
                }
            } else if (given instanceof Class) {
                Class<?> c = (Class<?>)given;
                if (c.isPrimitive()) {
                    return;
                }
                if (c.isArray()) {
                    explodeType(c.getComponentType(), classNames);
                    return;
                }
                classNames.add(c.getName());
            } else {
                /* Other Type implementations can be ignored. */
                tell(4, "Found a useless Type implementor:" +
                     given.getClass().getName());
            }
        }
    }

    /**
     * FieldWrapper contains a Field and provides a different implementation of
     * equals.  The default implementation is too stringent; classes from
     * discrete classloaders are never equal by its rules.
     */
    static class FieldWrapper implements Comparable<FieldWrapper> {
        private final Field field;
        private String[] reachedBy;

        private FieldWrapper(Field f, String[] reachedBy) {
            field = f;
            this.reachedBy = reachedBy;
        }

        public Type getGenericType() {
            return field.getGenericType();
        }

        public int getModifiers() {
            return field.getModifiers();
        }

        private String getName() {
            return field.getName();
        }

        private Class<?> getType() {
            return field.getType();
        }

        private String getDeclaringClass() {
            return field.getDeclaringClass().getName();
        }

        private boolean isStatic() {
            return Modifier.isStatic(field.getModifiers());
        }

        /*
         * A "significant" modifier is one that makes a difference to the
         * representation of a serialized object.
         * Possible modifiers are
         *
         *   public protected private abstract static final transient
         *   volatile synchronized native strictfp interface
         *
         * Of these, I deem the following to be insignificant:
         *
         *   public, protected, private, final, volatile, synchronized
         */
        private String getSignificantModifiers() {
            final List<String> squashList =
                Arrays.asList("public",
                              "protected",
                              "private",
                              "final",
                              "volatile",
                              "synchronized");
            final String modifierString = Modifier.toString(field.getModifiers());
            final List<String>modifiersList =
                new ArrayList<>(Arrays.asList(modifierString.split(" ")));
            modifiersList.removeAll(squashList);
            return String.join(" ", modifiersList);
        }

        @SuppressWarnings("unused")
        private boolean isTransient() {
            return Modifier.isTransient(field.getModifiers());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof FieldWrapper)) {
                return false;
            }
            FieldWrapper other = (FieldWrapper)obj;

            return toShortString().equals(other.toShortString());
        }

        @Override
        public int compareTo(FieldWrapper f) {
            return getName().compareTo(f.getName());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getName().hashCode();
            result = prime * result + getType().getName().hashCode();
            return result;
        }

        @Override
        public String toString() {
            return field.toGenericString();
        }

        public String toShortString() {
            String mods = getSignificantModifiers();
            if (! "".equals(mods)) {
                mods = mods + " ";
            }
            return mods + getGenericType() + " " + getName();
        }

        public String[] getReachedBy() {
            return reachedBy;
        }

        public boolean isEnumConstant() {
            return field.isEnumConstant();
        }
    }

    private void compareFields(ClassEntry ce1, ClassEntry ce2) {

        final List<FieldWrapper> fl1 = ce1.getAllFields();
        final List<FieldWrapper> fl2 = ce2.getAllFields();

        final List<FieldWrapper> onlyin1 = new ArrayList<FieldWrapper>(fl1);
        onlyin1.removeAll(fl2);
        final List<FieldWrapper> onlyin2 = new ArrayList<FieldWrapper>(fl2);
        onlyin2.removeAll(fl1);

        /*
         * Now we want to see if there are any duplications of field names in
         * the two "onlyin" lists, to see if a field with the same name has
         * changed its type.  We can't use set operations for this.  Use
         * Lists here because the order of changed1 and changed2 must match.
         */
        final List<FieldWrapper> changed1 = new ArrayList<FieldWrapper>();
        final List<FieldWrapper> changed2 = new ArrayList<FieldWrapper>();
        for (FieldWrapper fw1 : onlyin1) {
            for (FieldWrapper fw2 : onlyin2) {
                if (fw1.getName().equals(fw2.getName())) {
                    changed1.add(fw1);
                    changed2.add(fw2);
                }
            }
        }

        /*
         * Those in the changed lists need to be taken out of the onlyin lists.
         */
        onlyin1.removeAll(changed1);
        onlyin2.removeAll(changed2);

        /* Summarize */
        if (onlyin1.size() > 0) {
            tell(1, "In " + ce1.getName() +
                               ", these fields were DELETED.");
            for (FieldWrapper fw : onlyin1) {
                changes.recordFieldRemoved(fw);
                tell(1, "  " + fw.toString());
            }
        }

        if (onlyin2.size() > 0) {
            tell(1, "In " + ce1.getName() +
                               ", these fields were added.");
            for (FieldWrapper fw : onlyin2) {
                changes.recordFieldAdded(fw);
                tell(1, "  " + fw.toString());
            }
        }

        if (changed1.size() > 0) {
            tell(1, "In " + ce1.getName() +
                               ", these fields' types changed.");
            for (int i = 0; i < changed1.size(); i++) {
                FieldWrapper f1 = changed1.get(i);
                FieldWrapper f2 = changed2.get(i);
                changes.recordFieldChanged(f1, f2);
                tell(1, " " + f1.toString() + " changed to " + f2.toString());
            }
        }
    }

    /**
     * MethodWrapper contains a Method and provides a different implementation
     * of equals.  Methods are considered equal if their names, and the names
     * of their return types, and their parameter lists are the same.
     */
    static class MethodWrapper implements Comparable<MethodWrapper> {
        private final Method method;
        private String[] reachedBy;

        private MethodWrapper(Method m, String[] reachedBy) {
            method = m;
            this.reachedBy = reachedBy;
        }

        public Type[] getGenericParameterTypes() {
            return method.getGenericParameterTypes();
        }

        public Type getGenericReturnType() {
            return method.getGenericReturnType();
        }

        public boolean isStatic() {
            return Modifier.isStatic(method.getModifiers());
        }

        private String getName() {
            return method.getName();
        }

        private String getReturnType() {
            return method.getReturnType().getName();
        }

        private String[] getParameterTypes() {
            Class<?>[] parameterTypes = method.getParameterTypes();
            String[] typeStrings = new String[parameterTypes.length];

            for (int i = 0; i < parameterTypes.length; i++) {
                typeStrings[i] = parameterTypes[i].getName();
            }
            return typeStrings;
        }

        private String getDeclaringClass() {
            return method.getDeclaringClass().getName();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof MethodWrapper)) {
                return false;
            }

            MethodWrapper other = (MethodWrapper)obj;

            return toString().equals(other.toString());
        }

        @Override
        public int compareTo(MethodWrapper m) {
            return getName().compareTo(m.getName());
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getName().hashCode();
            result = prime * result + getReturnType().hashCode();
            for (String pt : getParameterTypes()) {
                result = prime * result + pt.hashCode();
            }
            return result;
        }

        @Override
        public String toString() {
            return method.toGenericString();
        }

        public String[] getReachedBy() {
            return reachedBy;
        }
    }

    /**
     * The logic of this method is nearly identical to compareFields.  Perhaps
     * they could be consolidated.
     */
    private void compareMethods(ClassEntry ce1, ClassEntry ce2) {
        final List<MethodWrapper> m1 = ce1.getAllMethods();
        final List<MethodWrapper> m2 = ce2.getAllMethods();

        final List<MethodWrapper> onlyin1 = new ArrayList<MethodWrapper>(m1);
        onlyin1.removeAll(m2);
        final List<MethodWrapper> onlyin2 = new ArrayList<MethodWrapper>(m2);
        onlyin2.removeAll(m1);

        final List<MethodWrapper> changed1 = new ArrayList<MethodWrapper>();
        final List<MethodWrapper> changed2 = new ArrayList<MethodWrapper>();

        for (MethodWrapper mw1 : onlyin1) {
            for (MethodWrapper mw2 : onlyin2) {
                if (mw1.getName().equals(mw2.getName())) {
                    changed1.add(mw1);
                    changed2.add(mw2);
                }
            }
        }

        onlyin1.removeAll(changed1);
        onlyin2.removeAll(changed2);

        /* Summarize */

        if (onlyin1.size() > 0) {
            tell(1, "In " + ce1.getName() +
                               ", these methods were deleted.");
            for (MethodWrapper mw : onlyin1) {
                changes.recordMethodRemoved(mw);
                tell(1, "  " + mw.toString());
            }
        }

        if (onlyin2.size() > 0) {
            tell(1, "In " + ce1.getName() +
                               ", these methods were added.");
            for (MethodWrapper mw : onlyin2) {
                changes.recordMethodAdded(mw);
                tell(1, "  " + mw.toString());
            }
        }

        if (changed1.size() > 0) {
            tell(1, "In " + ce1.getName() +
                               ", these methods' return types " +
                               "or argument lists changed.");
            for (int i = 0; i < changed1.size(); i++) {
                MethodWrapper mw1 = changed1.get(i);
                MethodWrapper mw2 = changed2.get(i);
                changes.recordMethodChanged(mw1, mw2);
                tell(1, " " + mw1.toString() + " changed to " + mw2.toString());
            }
        }
    }

    private boolean isIgnorableField(Field field) {
        int modifiers = field.getModifiers();

        /* Check for static modifier, unless it's an enum value. */
        if (Modifier.isStatic(modifiers) &&
            !(examineStaticFields || field.isEnumConstant())) {
                return true;
        }

        /*
         * Check for transient modifier.
         *
         * TODO: account for DPL annotations.
         * DPL has annotations that can override a declaration, to change
         * whether the field is persistent for DPL's purposes.  For example, a
         * field can be declared transient so that it doesn't get serialized
         * over RMI, but, with an annotation, the same field can be considered
         * persistent with respect to DPL.  I'm pretty sure that we don't
         * currently use this feature, which is why I goal-reduced this
         * analysis.
         */
        if (Modifier.isTransient(modifiers) && !examineTransientFields) {
            return true;
        }

        return false;
    }

    private boolean isIgnorableClassName(String s) {

        for (String p : ignorablePrefixes) {
            if (s.startsWith(p)) {
                return true;
            }
        }
        return false;
    }

    enum ChangeType { ClassNewlyPersistent,
                      ClassNoLongerPersistent,
                      ClassInheritanceChanged,
                      RemoteInterfaceAdded,
                      RemoteInterfaceRemoved,
                      FieldAdded,
                      FieldRemoved,
                      FieldChanged,
                      MethodAdded,
                      MethodRemoved,
                      MethodChanged }
    /**
     * A single change is represented by an object of this class, which is
     * serialized into a JSON string and stored for future reference.
     */
    public static class Change {

        ChangeType changeType;
        private String memberName; /* A field or method name, can be null. */
        private String oldDecl;    /* Old declaration of a field or method. */
        private String newDecl;    /* New declaration of a field or method. */

        public Change(ChangeType changeType,
                      String memberName,
                      String oldDecl,
                      String newDecl) {
            this.changeType = changeType;
            this.memberName = memberName;
            this.oldDecl = oldDecl;
            this.newDecl = newDecl;
        }

        public Change(ChangeType changeType) {
            this(changeType, null, null, null);
        }

        public Change(ChangeType changeType, String memberName) {
            this(changeType, memberName, null, null);
        }

        public Change() {
        }

        public ChangeType getChangeType() {
            return changeType;
        }

        public void setChangeType(ChangeType changeType) {
            this.changeType = changeType;
        }

        public String getMemberName() {
            return memberName;
        }

        public void setMemberName(String memberName) {
            this.memberName = memberName;
        }

        public String getOldDecl() {
            return oldDecl;
        }

        public void setOldDecl(String oldDecl) {
            this.oldDecl = oldDecl;
        }

        public String getNewDecl() {
            return newDecl;
        }

        public void setNewDecl(String newDecl) {
            this.newDecl = newDecl;
        }

        @Override
        public String toString() {
            String lf = "\n";
            try {
                lf = System.getProperty("line.separator");
            } catch (Exception t) { }

            StringBuilder r = new StringBuilder("    ChangeType: ");
            r.append(changeType);

            if (oldDecl != null) {
                r.append(lf);
                r.append("        OldDecl: ");
                r.append(oldDecl);
            }

            if (newDecl != null) {
                r.append(lf);
                r.append("        NewDecl: ");
                r.append(newDecl);
            }

            return r.toString();
        }

        /**
         * Compare two Changes, ignoring the value of "ok".
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (! (obj instanceof Change)) {
                return false;
            }

            Change other = (Change)obj;

            /*
             * There is always a change type; it shouldn't be null, but check
             * anyway.
             */
            if (changeType == null ?
                other.changeType != null :
                ! changeType.equals(other.changeType)) {

                return false;
            }

            /* Other fields can be null. */
            if (memberName == null ?
                other.memberName != null :
                ! memberName.equals(other.memberName)) {

                return false;
            }

            if (oldDecl == null ?
                other.oldDecl != null :
                ! oldDecl.equals(other.oldDecl)) {

                return false;
            }

            if (newDecl == null ?
                other.newDecl != null :
                ! newDecl.equals(other.newDecl)) {

                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + changeType.hashCode();
            result = prime * result +
                (memberName == null ? 0 : memberName.hashCode());
            result = prime * result +
                (oldDecl == null ? 0 : oldDecl.hashCode());
            result = prime * result +
                (newDecl == null ? 0 : newDecl.hashCode());
            return result;
        }
    }

    /* The changes of a single class. */
    public class ClassChanges {

        private String className;
        private String[] reachedBy;/* Path showing how the class was reached */
        private List<Change> changesInClass;
        public ClassChanges() {
            changesInClass = new ArrayList<Change>();
        }

        public ClassChanges(String className, String[] reachedBy) {
            this();
            this.className = className;
            this.reachedBy = reachedBy;
        }

        public void add(Change c) {
            if (changesInClass.contains(c)) {
                return;
            }
            changesInClass.add(c);
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String[] getReachedBy() {
            return reachedBy;
        }

        public void setReachedBy(String[] reachedBy) {
            this.reachedBy = reachedBy;
        }

        public List<Change> getChangesInClass() {
            return changesInClass;
        }

        public void setChangesInClass(List<Change> changesInClass) {
            this.changesInClass = changesInClass;
        }
    }

    public class Changes {

        public Map<String, ClassChanges> byClass = new TreeMap<>();

        private ClassChanges getOrCreate(String key, String[] reachedBy) {
            ClassChanges cs = byClass.get(key);
            if (cs == null) {
                cs = new ClassChanges(key, reachedBy);
                byClass.put(key, cs);
            }
            return cs;
        }

        public void recordClassNewlyPersistent(String name,
                                               String[] reachedBy) {
            recordChange(name, reachedBy,
                         new Change(ChangeType.ClassNewlyPersistent));

        }

        public void recordClassNoLongerPersistent(String name,
                                                  String[] reachedBy) {
            recordChange(name, reachedBy,
                         new Change(ChangeType.ClassNoLongerPersistent));
        }

        public void recordClassInheritanceChanged(String name,
                                                  String[] reachedBy,
                                                  String oldChain,
                                                  String newChain) {
            recordChange(name, reachedBy,
                         new Change(ChangeType.ClassInheritanceChanged,
                                    null, oldChain, newChain));
        }

        public void recordRemoteInterfaceAdded(String name,
                                               String[] reachedBy) {
            recordChange(name, reachedBy,
                         new Change(ChangeType.RemoteInterfaceAdded));
        }

        public void recordRemoteInterfaceRemoved(String name,
                                                 String[] reachedBy) {
            recordChange(name, reachedBy,
                         new Change(ChangeType.RemoteInterfaceRemoved));
        }

        public void recordFieldAdded(FieldWrapper fw) {
            String decl = fw.toShortString();
            if (fw.isEnumConstant()) {
                decl = decl.replace("static class", "(enum value)");
            }
            recordChange(fw.getDeclaringClass(), fw.getReachedBy(),
                         new Change(ChangeType.FieldAdded, fw.getName(), null,
                                    decl));
        }

        public void recordFieldRemoved(FieldWrapper fw) {
            recordChange(fw.getDeclaringClass(), fw.getReachedBy(),
                         new Change(ChangeType.FieldRemoved, fw.getName(),
                                    fw.toShortString(), null));
        }

        public void recordFieldChanged(FieldWrapper fw1, FieldWrapper fw2) {
            recordChange(fw1.getDeclaringClass(), fw1.getReachedBy(),
                         new Change(ChangeType.FieldChanged, fw1.getName(),
                                    fw1.toShortString(), fw2.toShortString()));
        }

        public void recordMethodAdded(MethodWrapper mw) {
            recordChange(mw.getDeclaringClass(), mw.getReachedBy(),
                         new Change(ChangeType.MethodAdded, mw.getName(), null,
                                    mw.toString()));
        }

        public void recordMethodRemoved(MethodWrapper mw) {
            recordChange(mw.getDeclaringClass(), mw.getReachedBy(),
                         new Change(ChangeType.MethodRemoved, mw.getName(),
                                    mw.toString(), null));
        }

        public void recordMethodChanged(MethodWrapper mw1, MethodWrapper mw2) {
            recordChange(mw1.getDeclaringClass(), mw1.getReachedBy(),
                         new Change(ChangeType.MethodChanged, mw1.getName(),
                                    mw1.toString(), mw2.toString()));
        }

        /**
         * Add the new item to the list only if an equivalent item
         * is not already in the list.
         */
        private void recordChange(String className,
                                  String[] reachedBy,
                                  Change newChange) {

            ClassChanges cs = getOrCreate(className, reachedBy);

            cs.add(newChange);
        }
    }

    /**
     * Convert the Changes object to a JSON string, and save the result.
     */
    private static void saveChanges(File changesFile, Changes c) throws IOException {

        JsonUtils.writeFile(changesFile, c, true); // pretty print to file
    }

    /**
     * For debug and progress logging.
     */
    private void tell(int level, String message) {
        if (level <= verbose) {
            System.err.println(message);
        }
    }
}
