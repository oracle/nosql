/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */
package oracle.nosql.query;

import java.util.Properties;

/*
 * The RowPropsSetup is the setup class of test rowprops in qtf test.
 *
 * Note:
 * This RowPropsSetup in KV qtf test creates dummy tables before create test
 * table to make sure the test table can be assigned with fixed table id(s).
 *
 * When using proxy, no table id returned to client, so unable to do same
 * thing as that of KV qtf test. Actually, the rowprops test was excluded from
 * the proxy-based qtf test, adding this case is just to pass through the
 * initializing of QTSuiteCloud that parses the test configuration of all tests.
 */
public class RowPropsSetup extends QTDefaultImpl {

    @Override
    public void setConfigProperties(Properties properties) {
        super.setConfigProperties(properties);
        if (!configProperties.containsKey("before-ddl-file")) {
            configProperties.setProperty("before-ddl-file", "before.ddl");
        }
        if (!configProperties.containsKey("after-ddl-file")) {
            configProperties.setProperty("after-ddl-file", "after.ddl");
        }
    }
}
