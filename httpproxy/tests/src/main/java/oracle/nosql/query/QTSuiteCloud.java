/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2018 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.query;

import java.io.File;
import oracle.nosql.common.qtf.QTOptions;
import oracle.nosql.common.qtf.QTSuite;

/**
 * Class representing test suite.
 */
public class QTSuiteCloud extends QTSuite {

    public static final String PKGNAME = "oracle.nosql.query.";

    QTSuiteCloud(QTOptions opts, File configFile) {
        super(opts, configFile, PKGNAME);
    }

    @Override
    protected boolean getRunCase(String prop) {
        return prop.startsWith(RUN_PREFIX);
    }
}
