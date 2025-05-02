/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.File;
import java.util.Properties;

/**
 * Code that runs Before a suite of tests can be customized by a new class
 * that must extend this interface and it must be referenced using:
 *
 * before-class = full.java.class.Name
 *
 * If this property is specified, before-ddl-file and before-data-file are
 * ignored.
 *
 * When before-class property is not specified, the default class used is:
 * qt.framework.QTDefaultImpl. This class can be extended by an implementation
 * and override only some aspects of it's implementation.
 *
 * Custom implementation classes should live in rest/query/src/qt dir.
 */
public interface QTBefore {

    public void setOptions(QTOptions opts);
    public void setConfigFile(File configFile);
    public void setConfigProperties(Properties configProperties);

    public void before();
}
