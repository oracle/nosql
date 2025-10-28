/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.query;

import java.io.File;

import oracle.nosql.common.qtf.QTCase;
import oracle.nosql.common.qtf.QTFactory;
import oracle.nosql.common.qtf.QTOptions;
import oracle.nosql.common.qtf.QTRun;
import oracle.nosql.common.qtf.QTSuite;

class QTFactoryCloud implements QTFactory {

    @Override
    public QTSuite createQTSuite(QTOptions opts, File configFile) {
        return new QTSuiteCloud(opts, configFile);
    }

    @Override
    public QTRun createQTRun() {
        return new QTRun();
    }

    @Override
    public QTCase createQTCase() {
        return new QTCaseCloud();
    }
}
