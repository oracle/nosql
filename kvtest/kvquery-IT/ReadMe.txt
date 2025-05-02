Query Test Framework
---------------------

This a test framework for the DML query feature. By default, query cases are
unzipped from qtf.jar which is built from nosql.common and copy to
kvstore/test/query/cases. If -Dqtf.cases is set to <path to query cases>,
for example, /scratch/nosql.common/qtf/cases, QTF test will use the cases
in /scratch/nosql.common/qtf/cases instead of unzipping query cases from qtf.jar.

How to run
----------

1) Using JUnit execution:

a) From ant task: in the kvstore dir execute:
    ant test -Dtestcase=qt.framework.QTest
  or with a filter:
    ant test -Dtestcase=qt.framework.QTest -Dtest.filter=Table/q/7
  or specify the query cases:
    ant test -Dtestcase=qt.framework.QTest -Dqtf.cases=/scratch/nosql.common/qtf/cases

b) From JUnit (using JUnit runner inside a regular Java app):
    java -ea -Dtestdestdir=/path/to/kvstore/build/kvsandbox -cp ... qt.framework.QTest
  or with a filter:
    java -ea -Dtestdestdir=/path/to/kvstore/build/kvsandbox -cp ... -Dtest.filter=Table/q/7 qt.framework.QTest
  or specify the query cases:
    java -ea -Dtestdestdir=/path/tokvstore/build/kvsandbox -cp ... -Dqtf.cases=/scratch/nosql.common/qtf/cases qt.framework.QTest

NOTE: be careful running standalone test cases from a cases directory. Some of
them have side effects in that they may expect results from a different (smaller
numbered) case to affect results. Watch out for inserts and deletes.

JVM params available when running under JUnit or Ant:
 -Dtestdestdir= must be specified.
 -Dtest.filter=<string>  Filters the test cases to contain the <filter> string.
 -Dtest.filterOut=<string>  Filters out the test cases to contain the string.
     Default value is "idc_". Use "" to run idc_ tests.
 -Dtest.verbose=on       Turns on verbose for junit.
 -Dtest.qt.verbose=on    Turns on verbose for QTF (Query Test Framework).
 -Dtest.updateQueryPlans=on Regenerates the result files that contain different
     than expected query plans.
 -Dtest.actual=true Generate .actual files next to expected result files with
     the actual output.
 -Dtest.queryonclient=true Runs the iterator code on the client, useful for
     debugging.
 -Dtest.traceLevel=<byte> Sets the trace level of the query in ExecuteOptions.
 -Dtest.batchSize=<int> Sets the number of results per request in ExecuteOptions.
 -Dtest.readKBLimit=<int> Sets the max number of KBs that a query incarnation can
  consume while running at an RN.
 -Dtest.qtf.store=<external store name> For example, -Dtest.qtf.store=mystore
 -Dtest.qtf.hostport=<external store host port pair> For example,
  -Dtest.qtf.hostport=localhost:5000
 -Dqtf.cases=<path to query cases> For example,
     -Dqtf.cases=/scratch/nosql.common/qtf/cases
 -Dtest.mrtable Adds regions to all "create table" statements in before.ddl,
  except for those with identity column and ttl, so multi-region tables will
  be created and used in the test. Tests in test/query/mrtableExpectedFailure
  will be filtered out since they have child tables.

2) Or using a regular Java app (using own QTF runner):
  Note: -Dtestdestdir=/path/to/kvstore/build/kvsandbox must be specified:

    java -ea -Dtestdestdir=/path/to/kvstore/build/kvsandbox -cp ... qt.framework.RunQueryTests
or with a filter:
    java -ea -Dtestdestdir=/path/to/kvstore/build/kvsandbox -cp ... qt.framework.RunQueryTests -filter=Table/q/7

Other valid params are when running standalone:
  -base-dir <dir-path> Base directory of the query tests, usually
     ./kvstore/test/query/cases.
  -filter <string> Filter relative full  test name to contain the given string.
  -filterout=<string>  Filters out the test cases to contain the string.
      Default value is "idc_". Use "" to run idc_ tests.
  -verbose Enable verbose output.
  -updateQueryPlans Regenerates the result files that contain different than
     expected query plans.
  -actual=true Generate .actual files next to expected result files with
     the actual output.

Adding new tests
----------------
New tests should be added under nosql.common/qtf/cases/. Individual tests are
grouped by suites, usually by the feature they are testing. Suite directory
must contain a test.config file.

test.config file
----------------
Valid properties:
    before-* and after-* props are executed only if a run-* prop exists in
    this file or another config.test file uses this config.test file as a
    dependency.

    before-class = full.java.class.Name - must implement qt.framework.QTBefore,
        creates a new instance and executes before() before run-* statements
        If this is specified, before-ddl-file and before-data-file are
        ignored.
    before-ddl-file   = file-name.ddl  - executes DDL statements
    before-data-file  = file-name.data - executes put with included records

    run-* = query-dir-name( dep-dir1, dep-dir2, //dep-dir3 ) = result-dir
        dep-dir params are optional. use query-dir-name() for no dependencies.
        Triggers before-* props in this file. Runs before-* in all
        dependency dirs and runs queries in all .q files from
        query-dir-name. Dependencies that start with // are resolved
        starting from query/cases dir. Expected results must be in files with
        the same name as the query file but ending in '.r' in result-dir.
        Note: result-dir can be the same dir as the query-dir.

    compile-* = query-dir-name( dep-dir1, dep-dir2, //dep-dir3 ) = result-dir
        Same as run-* but only compiles the query checking for the Query Plan.

    after-class = full.java.class.Name - must implement qt.framework.QTAfter,
        creates a new instance and executes after() after run-* statements
        If this is specified, after-ddl-file property is ignored.
    after-ddl-file   = file-name.ddl  - executes DDL statements

    var-* =

      Examples:
      var-$foo = 3
      var-$bar = type:int:3
      var-$str = "xyz"

      Specifies the name of a bind variable, its value, and optionlly its type.
      If a query declares a variable with the same name, the value specified
      here is used as the bind value for the query variable.

Only test.config files with "run-*" entries will trigger test executions.

Ddl files
---------
Files that are referenced from entries: before-ddl-file = file-name.ddl must
 contain valid DDL statements to be executed before a suite of tests are run.

Statements must be delimited by at least an empty line. Statements can use
multiple consecutive lines.

Data files
----------
Files that are referenced from entries: before-data-file = file-name.data must
 contain blocks formed of block header and block content.
Block headers must  contain 'Table: table-name'.
Block content is a list of records in JSON format that are split by empty
lines.

Result files
------------
Result files have 3 parts: 1) optional comment lines,
                           2) result-type line and
                           3) result-content

1) Comment lines start with \s*#

2) Result-type line is mandatory and can be one of:
   Compiled-query-plan, Compile-exception, Runtime-exception,
   Unordered-result or Ordered-result

3) Result-content:
 - for Compiled-query-plan the rest of the file must contain the serialized
 query plan.
 - for Compile-exception or Runtime-exception the rest of the file may contain
an exception message.
 - for Unordered-result or Ordered-result the rest of the file contains
un/ordered result records. One record per line. Each line is
trimmed of spaces.

For all files
-------------
New line is considered only if char '\n' is present.
