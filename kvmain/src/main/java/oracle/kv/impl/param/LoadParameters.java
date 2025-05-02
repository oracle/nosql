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

package oracle.kv.impl.param;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import oracle.kv.impl.security.util.FileSysUtils;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Manage and bundle up a set of ParameterMaps. Used to transport multiple
 * ParameterMaps across the network, and to load parameters from a
 * configuration file. Note that each map must have a name and a type associated
 * with it.
 *
 * The config.xml represents a simple"schema""
 * <config version="1">
 *   <component name="blah" type="paramtype">
 *     <property name="n" value="v" type="t"/>
 *     <property name="n" value="v" type="t"/>
 *     ...
 *   </component>
 *   <component name="foo">
 *     ...
 *   </component>
 *   ...
 * </config>
 *
 * The bundle contains at most one map with a given name.
 */
public class LoadParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    /* This persisted field is not used at runtime. */
    private List<ParameterMap> maps;
    private int version;

    /**
     * Parameters are kept in a sorted map while in memory. We sort just to
     * aid navigation of the configuration file. The parameter maps
     * are persisted via the maps field for compatibility.
     */
    private transient SortedMap<String, ParameterMap> parameterMaps;

    /**
     * Read parameters by type from file. Also check if file is writable,
     * if so, attempts to set file as read only.
     */
    public static synchronized
        LoadParameters getParameters(File file, Logger logger) {
        final LoadParameters lp = new LoadParameters();
        lp.load(file, false, true, true, logger);
        return lp;
    }

    /**
     * Read parameters by type from file. Also check if file is writable,
     * if so, attempts to set file as read only.
     */
    public static synchronized LoadParameters getParametersByType(File file) {
        return getParametersByType(file, true, null);
    }

    /**
     * Read parameters by type from file. Also check if file is writable,
     * if so, attempts to set file as read only.
     */
    public static synchronized
        LoadParameters getParametersByType(File file, Logger logger) {
        final LoadParameters lp = new LoadParameters();
        lp.load(file, true, true, true, logger);
        return lp;
    }

    public LoadParameters() {
        parameterMaps = new TreeMap<String, ParameterMap>();
        version = ParameterState.PARAMETER_VERSION;
    }

    public static synchronized LoadParameters getParameters(File file,
                                               boolean checkReadOnly,
                                               Logger logger) {
        final LoadParameters lp = new LoadParameters();
        lp.load(file, false, true, checkReadOnly, logger);
        return lp;
    }

    public static synchronized LoadParameters getParametersByType(File file,
                                                     boolean checkReadOnly) {
        return getParametersByType(file, checkReadOnly, null);
    }

    public static synchronized LoadParameters getParametersByType(File file,
                                                     boolean checkReadOnly,
                                                     Logger logger) {
        final LoadParameters lp = new LoadParameters();
        lp.load(file, true, true, checkReadOnly, logger);
        return lp;
    }

    /**
     * Save parameters to given file and set this file read only.
     */
    public void saveParameters(File file) {
        saveParameters(file, true, null /* logger */);
    }

    /**
     * Save parameters to given file and set this file read only.
     */
    public void saveParameters(File file, Logger logger) {
        synchronized(this.getClass()) {
        saveParameters(file, true, logger);
        }
    }

    /**
     * In order to make this modification more atomic, write first to a
     * temporary file and rename.  Because of file system semantics on
     * some platforms the rename can fail.  Handle that with a retry.
     * This will only fail if there is a reader.  Writers must be
     * synchronized by the callers (e.g. the SNA).
     * @param readonly if set this file read only. Ignore the error of
     * setting file permission.
     */
    public void saveParameters(File file, boolean readonly, Logger logger) {
        PrintWriter writer = null;
        File temp = null;
        try {
            temp = File.createTempFile(file.getName(), null,
                                       file.getParentFile());
            final FileOutputStream fos = new FileOutputStream(temp);
            writer = new PrintWriter(fos);
            writer.print(
                "<!-- Do not edit this file, it is machine generated. -->\n");
            writer.printf("<config version=\"%d\">\n", version);
            for (ParameterMap map : parameterMaps.values()) {
                map.write(writer);
            }
            writer.printf("</config>\n");
        } catch (Exception e) {
            throw new IllegalStateException("Problem creating config file: " +
                                            temp + ": " + e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        if (temp != null) {
            final int nretries = 5;
            final int sleepMs = 200;
            for (int i = 0; i < nretries; i++) {
                if (temp.renameTo(file)) {
                    if (readonly) {
                        if (makeReadAccessOnly(file)) {
                            return;
                        }
                        logMessage(logger, Level.FINE,
                                   "Unable to set config file " + file +
                                   " read only");
                    }
                    return;
                }

                /**
                 * It seems that on Windows renameTo() often fails without a
                 * delete.  This shows up in tests, at least.  This is not a
                 * high performance path, so if it fails once, do the explicit
                 * delete on Windows.  This should not be unconditional because
                 * it makes the operation less atomic, leaving a small window
                 * available for a read failure.
                 */
                final String os = System.getProperty("os.name");
                if (os.indexOf("Windows") != -1) {
                    file.delete();
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (Exception ignored) {
                }
            }
        }
        throw new IllegalStateException("Unable to save config file: " + file);
    }

    List<ParameterMap> getMaps() {
        return new ArrayList<ParameterMap>(parameterMaps.values());
    }

    /**
     * Adds the specified map if not null. If a map with the same name already
     * exist it is replaced. The map must have a non-null name and type.
     */
    public void addMap(ParameterMap map) {
        if (map == null) {
            return;
        }
        if ((map.getName() == null) || (map.getType() == null)) {
            throw new IllegalArgumentException("ParameterMap must have a " +
                                               "name and type");
        }
        parameterMaps.put(map.getName(), map);
    }

    /**
     * Removes maps with the specified name. If a map was removed it is
     * returned, otherwise null is returned.
     */
    public ParameterMap removeMap(String name) {
        return parameterMaps.remove(name);
    }

    /**
     * Removes all maps with the specified type. If no maps are removed null
     * is returned. If one or more map is removed there is no guarantee which
     * map is returned.
     */
    public ParameterMap removeMapByType(String type) {
        ParameterMap map = null;
        final Iterator<ParameterMap> itr = parameterMaps.values().iterator();
        while (itr.hasNext()) {
            final ParameterMap canidate = itr.next();
            if (type.equals(canidate.getType())) {
                itr.remove();
                map = canidate;
            }
        }
        return map;
    }

    /**
     * Gets the parameter map with the specified name and type. If no map
     * matches null is returned.
     */
    public ParameterMap getMap(String name, String type) {
        final ParameterMap map = parameterMaps.get(name);
        if ((map != null) && type.equals(map.getType())) {
            return map;
        }
        return null;
    }

    /**
     * Gets a map of the specified type. If no map matches null is returned.
     * If more than one map of the same type is present, there is no guarantee
     * on which is returned.
     */
    public ParameterMap getMapByType(String type) {
        for (ParameterMap map : parameterMaps.values()) {
            if (type.equals(map.getType())) {
                return map;
            }
        }
        return null;
    }

    public List<ParameterMap> getAllMaps(String type) {
        final ArrayList<ParameterMap> list = new ArrayList<ParameterMap>();
        for (ParameterMap map : parameterMaps.values()) {
            if (type.equals(map.getType())) {
                list.add(map);
            }
        }
        return list;
    }

    /**
     * Returns the map with the specified name. If no map is found null is
     * returned.
     */
    public ParameterMap getMap(String name) {
        return parameterMaps.get(name);
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * Given an abstract file, attempt to change permissions to remove all
     * write access.
     * @param f a File referencing a file or directory on which permissions are
     * to be changed.
     * @return true if the permissions were successfully changed
     */
    public static boolean makeReadAccessOnly(File f) {

        if (!f.exists()) {
            return false;
        }

        /* We don't support setting file permission on windows.
         * To allow user doing experiment on Windows, only set
         * file read only on Unix-based OS.
         */
        final String os = System.getProperty("os.name");
        if (os.indexOf("Windows") >= 0) {
            return true;
        }
        final FileSysUtils.Operations osOps =
            FileSysUtils.selectOsOperations();

        try {
            return osOps.makeReadAccessOnly(f);
        } catch (IOException ioe) {
            return false;
        }
    }

    private void load(File file, boolean useTypes, boolean ignoreUnknown,
                      boolean checkReadOnly, Logger logger) {
        InputStream is = null;
        try {
            /*
             * Check if configuration file is read only, which might not be
             * read only because upgrading from previous version or users
             * changed the file permission. Attempts to change it to read only,
             * ignoring the failure here to make sure parameters can be loaded.
             */
            if (checkReadOnly && !file.isDirectory() && file.canWrite()) {
                if (!makeReadAccessOnly(file)) {
                    logMessage(logger, Level.FINE,
                               "Reading a config file " + file +
                               ", attempts to change it as read only but fail");
                }
            }
            final URL url = file.toURI().toURL();
            final SAXParserFactory factory = SAXParserFactory.newInstance();
            final XMLReader xr = factory.newSAXParser().getXMLReader();
            final ConfigHandler handler =
                new ConfigHandler(useTypes, ignoreUnknown, this, logger);
            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);
            is = url.openStream();
            xr.parse(new InputSource(is));
        } catch (SAXParseException e) {
            final String msg = "Error while parsing line " + e.getLineNumber() +
                    " of " + file + ": " + e.getMessage();
            throw new IllegalStateException(msg, e);
        } catch (SAXException e) {
            throw new IllegalStateException("Problem with XML: " + e, e);
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } catch (MalformedURLException me) {
            throw new IllegalStateException
                ("Could not translate file to URL: " + file, me);
        } catch (IOException io) {
            throw new IllegalStateException
                ("IOException parsing file: " + file + ": " + io, io);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private void logMessage(Logger logger, Level level, String msg) {
        if (logger != null) {
            logger.log(level, msg);
        }
    }

    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        parameterMaps = new TreeMap<String, ParameterMap>();

        if (maps == null) {
            return;
        }

        for (ParameterMap map : maps) {
            parameterMaps.put(map.getName(), map);
        }
        maps = null;
    }

    private void writeObject(ObjectOutputStream out)
            throws IOException {

        maps = getMaps();
        out.defaultWriteObject();
        maps = null;
    }

    @Override
    public String toString() {
        return parameterMaps.toString();
    }

    private static class ConfigHandler extends DefaultHandler {
        Locator locator;
        ParameterMap curMap;
        final boolean useTypes;
        final boolean ignoreUnknown;
        final Logger logger;
        final LoadParameters lp;

        private ConfigHandler(boolean useTypes,
                              boolean ignoreUnknown,
                              LoadParameters lp,
                              Logger logger) {
            this.logger = logger;
            this.useTypes = useTypes;
            this.ignoreUnknown = ignoreUnknown;
            this.lp = lp;
        }

        @Override
        public void startElement(String uri,
                                 String localName,
                                 String qName,
                                 Attributes attributes)
            throws SAXException {

            if (qName.equals("config")) {
                /* get version */
                final String stringVersion = attributes.getValue("version");
                if (stringVersion == null) {
                    throw new SAXParseException
                        ("config element must specify version",
                         locator);
                }
                lp.setVersion(Integer.parseInt(stringVersion));
            } else if (qName.equals("component")) {
                final String curComponent = attributes.getValue("name");
                final String curType = attributes.getValue("type");
                final String validateString = attributes.getValue("validate");
                boolean validate = true;
                if (validateString != null) {
                    validate = Boolean.parseBoolean(validateString);
                }
                curMap = new ParameterMap(curComponent, curType,
                                          validate, lp.getVersion());
                /* Check for a badly formed component tag. */
                if (curComponent == null || curType == null) {
                    throw new SAXParseException
                        ("component element must specify name and type",
                         locator);
                }
            } else if (qName.equals("property")) {
                final String name = attributes.getValue("name");
                final String value = attributes.getValue("value");
                if (attributes.getLength() < 2 ||
                    attributes.getLength() > 3 ||
                    name == null ||
                    value == null) {
                    throw new SAXParseException
                        ("property element must only have 'name', 'value'" +
                         " and (optional) 'type' attributes", locator);
                }

                if (curMap == null) {
                    throw new SAXParseException
                        ("property elements are not allowed at global scope",
                         locator);
                }

                if (curMap.exists(name)) {
                    throw new SAXParseException
                        ("Duplicate property: " + name, locator);
                }

                final String type = attributes.getValue("type");
                Parameter param = null;
                if (useTypes) {
                    if (type == null) {
                        throw new SAXParseException
                            ("type attribute required on property element in" +
                             " this path", locator);
                    }
                    param = Parameter.createKnownType(name, value, type);
                    if (param != null) {
                        curMap.put(param);
                    } else if (logger != null) {
                        logger.warning("Could not create parameter: " +
                                       name);
                    }
                } else {
                    if (!curMap.setParameter(name, value, type,
                                             ignoreUnknown)) {
                        if (logger != null) {
                            logger.warning("Ignoring unknown parameter: " +
                                           name);
                        }
                    }
                }
            } else {
                throw new SAXParseException
                    ("Unknown element '" + qName + "'", locator);
            }
        }

        @Override
        public void characters(char ch[], int start, int length)
            throws SAXParseException {
            /* ignore content and whitespace */
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXParseException {

            if (qName.equals("component")) {
                lp.addMap(curMap);
                curMap = null;
            } else if (qName.equals("property")) {
                /* nothing to do */
            }
        }

        @Override
        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }
    }
}
