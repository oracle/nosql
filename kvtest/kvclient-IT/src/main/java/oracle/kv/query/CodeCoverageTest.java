/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.query;

import oracle.kv.TestBase;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.query.compiler.CompilerAPI;
import org.junit.Test;

/**
 * Test to improve code coverage of the generated KVQL parser code.
 */
public class CodeCoverageTest extends TestBase {

    final static TableMetadata metadata = new TableMetadata(false);

    private static final String[] queries = {
        "DECLARE",
        "DESC",
        "GRANT",
        "REVOKE",
        "SELECT",
        "SHOW",
        "ALTER TABLE",
        "ALTER USER",
        "CREATE FULLTEXT",
        "CREATE INDEX",
        "CREATE ROLE",
        "CREATE TABLE",
        "CREATE USER",
        "DECLARE $",
        "DESC INDEX",
        "DESC TABLE",
        "DROP INDEX",
        "DROP ROLE",
        "DROP TABLE",
        "DROP USER",
        "SHOW ROLE",
        "SHOW TABLE",
        "SHOW USER",
        "ALTER TABLE ACCOUNT",
        "ALTER USER 1badId",
        "CREATE INDEX 1badId",
        "CREATE TABLE 1badId",
        "CREATE USER ACCOUNT",
        "CREATE USER 1badId",
        "DECLARE $ ACCOUNT",
        "DESC TABLE ACCOUNT",
        "DESCRIBE TABLE id",
        "DROP INDEX 1badId",
        "DROP TABLE 1badId",
        "DROP USER 1badId",
        "GRANT ACCOUNT ON",
        "GRANT ALL PRIVILEGES TO",
        "GRANT 1badId ON",
        "REVOKE ACCOUNT ON",
        "REVOKE ALL PRIVILEGES FROM",
        "REVOKE 1badId ON",
        "SHOW ROLES ACCOUNT",
        "SHOW USER 1badId",
        "DESCRIBE TABLE IF //",
        "DROP INDEX ACCOUNT ON",
        "DROP TABLE ACCOUNT ACCOUNT",
        "GRANT ACCOUNT ON ACCOUNT",
        "ACCOUNT ACCOUNT ACCOUNT ACCOUNT ACCOUNT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id ACCOUNT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id COMMENT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id , 1badId",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id ) ACCOUNT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id ) COMMENT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id ) ES_SHARDS",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { \"a\" : [-1," +
            "true, false, null,\"\", {}, []]} )",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id [], id {}, " +
            "id {\"a\" : [-1,true,false,null,\"\", {}, []]} )",
        "ALTER TABLE table_name ( ADD id long, DROP id, MODIFY id long)",
        "ALTER USER id IDENTIFIED BY 'NoSql00__s' " +
            "RETAIN CURRENT PASSWORD CLEAR " +
            "RETAINED PASSWORD PASSWORD EXPIRE PASSWORD LIFETIME 1 DAYS " +
            "ACCOUNT LOCK",
        "CREATE FULLTEXT INDEX IF NOT EXISTS indx ON t" +
            "( id { \"a\":[-1,true,false,null,\"\",{},[]]}) ES_SHARDS=1 " +
            "ES_REPLICAS=2 COMMENT '';",
        "CREATE FULLTEXT INDEX IF NOT EXISTS indx ON t" +
            "( id [], id {}, id {\"a\" : [-1,true,false,null,\"\", {}, []]})" +
            " ES_SHARDS=1 ES_REPLICAS=2 COMMENT '';",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { ACCOUNT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } ACCOUNT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } )",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } , 1badId",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } , CLEAR RETAINED",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } ) ACCOUNT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } ) COMMENT",
        "CREATE FULLTEXT INDEX index_name ON table_name ( id { } ) ES_SHARDS",
        "GRANT id USER id",
        "GRANT id USER 'id'",
        "GRANT id ROLE id",
        "CREATE USER ACCOUNT",
        "CREATE USER FORCE_INDEX",
        "CREATE USER 1badId",
        "CREATE USER ACCOUNT IDENTIFIED",
        "CREATE USER ACCOUNT IDENTIFIED BY",
        "SELECT ACCOUNT",
        "SELECT (",
        "SELECT [",
        "SELECT +",
        "SELECT 1badId",
        "SELECT ACCOUNT ACCOUNT",
        "SELECT ACCOUNT AND",
        "SELECT ACCOUNT AS",
        "SELECT ACCOUNT FROM",
        "SELECT ACCOUNT (",
        "SELECT ACCOUNT .",
        "SELECT ( ACCOUNT",
        "SELECT ( (",
        "SELECT ( [",
        "SELECT ( +",
        "SELECT ( 1badId",
        "SELECT [ ACCOUNT",
        "SELECT [ (",
        "SELECT [ [",
        "SELECT [ +",
        "SELECT [ 1badId",
        "SELECT $ 1badId",
        "SELECT + (",
        "SELECT + [",
        "SELECT + +",
        "SELECT + 1badId",
        "SELECT 1badId (",
        "SELECT * FROM FORCE_INDEX",
        "SELECT * FROM t WHERE FORCE_INDEX",
        "SELECT * FROM t WHERE (",
        "SELECT * FROM t WHERE [",
        "SELECT * FROM t WHERE +",
        "SELECT * FROM t WHERE 1badId",
        "SELECT * FROM ACCOUNT FORCE_INDEX",
        "SELECT * FROM t WHERE ACCOUNT AND",
        "SELECT * FROM t WHERE ACCOUNT ORDER",
        "SELECT * FROM ACCOUNT $",
        "SELECT * FROM t WHERE ACCOUNT (",
        "SELECT * FROM t WHERE ACCOUNT .",
        "SELECT * FROM ACCOUNT CLEAR RETAINED",
        "SELECT * FROM t WHERE ORDER BY",
        "SELECT * FROM t WHERE ( ACCOUNT",
        "SELECT * FROM t WHERE ( (",
        "SELECT * FROM t WHERE ( [",
        "SELECT * FROM t WHERE ( +",
        "SELECT * FROM t WHERE ( 1badId",
        "SELECT * FROM t WHERE ( CLEAR RETAINED",
        "SELECT * FROM t WHERE [ ACCOUNT",
        "SELECT * FROM t WHERE [ (",
        "SELECT * FROM t WHERE [ [",
        "SELECT * FROM t WHERE [ +",
        "SELECT * FROM t WHERE [ 1badId",
        "SELECT * FROM t WHERE [ CLEAR RETAINED",
        "SELECT * FROM t WHERE $ 1badId",
        "SELECT * FROM t WHERE + (",
        "SELECT * FROM t WHERE + [",
        "SELECT * FROM t WHERE + +",
        "SELECT * FROM t WHERE + 1badId",
        "SELECT * FROM t WHERE + CLEAR RETAINED",
        "SELECT * FROM CLEAR RETAINED ORDER",
        "SELECT * FROM CLEAR RETAINED WHERE",
        "SELECT * FROM t WHERE 1badId (",
        "SELECT * FROM ACCOUNT ORDER BY",
        "SELECT * FROM ACCOUNT WHERE (",
        "SELECT * FROM ACCOUNT WHERE [",
        "SELECT * FROM ACCOUNT WHERE +",
        "SELECT * FROM ACCOUNT WHERE 1badId",
        "SELECT * FROM ACCOUNT WHERE CLEAR RETAINED",
        "SELECT * FROM t WHERE ACCOUNT ( (",
        "SELECT * FROM t WHERE ACCOUNT ( [",
        "SELECT * FROM t WHERE ACCOUNT ( +",
        "SELECT * FROM t WHERE ACCOUNT ( 1badId",
        "SELECT * FROM t WHERE ACCOUNT ( CLEAR RETAINED",
        "SELECT * FROM t WHERE ACCOUNT [ :",
        "SELECT * FROM t WHERE ACCOUNT . (",
        "SELECT * FROM t WHERE ORDER BY (",
        "SELECT * FROM t WHERE ORDER BY [",
        "SELECT * FROM t WHERE ORDER BY +",
        "SELECT * FROM t WHERE ORDER BY 1badId",
        "SELECT * FROM t WHERE ORDER BY CLEAR RETAINED",
        "SELECT * FROM t WHERE WHERE + CLEAR RETAINED",
        "SELECT * FROM t WHERE ( ACCOUNT (",
        "SELECT * FROM t WHERE ( ACCOUNT .",
        "SELECT * FROM t WHERE ( ( ACCOUNT",
        "SELECT * FROM t WHERE ( ( (",
        "SELECT * FROM t WHERE ( ( [",
        "SELECT * FROM t WHERE ( ( +",
        "SELECT * FROM t WHERE ( ( 1badId",
        "SELECT * FROM t WHERE ( ( CLEAR RETAINED",
        "SELECT * FROM t WHERE ( [ ACCOUNT",
        "SELECT * FROM t WHERE ( [ (",
        "SELECT * FROM t WHERE ( [ [",
        "SELECT * FROM t WHERE ( [ +",
        "SELECT * FROM t WHERE ( [ 1badId",
        "SELECT * FROM t WHERE ( [ CLEAR RETAINED",
        "SELECT * FROM t WHERE ( $ 1badId",
        "SELECT * FROM t WHERE ( + (",
        "SELECT * FROM t WHERE ( + [",
        "SELECT * FROM t WHERE ( + +",
        "SELECT * FROM t WHERE ( + 1badId",
        "SELECT * FROM t WHERE ( + CLEAR RETAINED",
        "SELECT * FROM t WHERE ( 1badId (",
        "SELECT * FROM t WHERE [ ACCOUNT (",
        "SELECT * FROM t WHERE [ ACCOUNT .",
        "SELECT * FROM t WHERE [ ( ACCOUNT",
        "SELECT * FROM t WHERE [ ( (",
        "SELECT * FROM t WHERE [ ( [",
        "SELECT * FROM t WHERE [ ( +",
        "SELECT * FROM t WHERE [ ( 1badId",
        "SELECT * FROM t WHERE [ ( CLEAR RETAINED",
        "SELECT * FROM t WHERE [ [ ACCOUNT",
        "SELECT * FROM t WHERE [ [ (",
        "SELECT * FROM t WHERE [ [ [",
        "SELECT * FROM t WHERE [ [ +",
        "SELECT * FROM t WHERE [ [ 1badId",
        "SELECT * FROM t WHERE [ [ CLEAR RETAINED",
        "SELECT * FROM t WHERE [ $ 1badId",
        "SELECT * FROM t WHERE [ + (",
        "SELECT * FROM t WHERE [ + [",
        "SELECT * FROM t WHERE [ + +",
        "SELECT * FROM t WHERE [ + 1badId",
        "SELECT * FROM t WHERE [ + CLEAR RETAINED",
        "SELECT * FROM t WHERE [ 1badId (",
        "SELECT * FROM t WHERE $ . 1badId",
        "SELECT * FROM t WHERE + ACCOUNT (",
        "SELECT * FROM t WHERE + ACCOUNT .",
        "SELECT * FROM t WHERE + ( ACCOUNT",
        "SELECT * FROM t WHERE + ( (",
        "SELECT * FROM t WHERE + ( [",
        "SELECT * FROM t WHERE + ( +",
        "SELECT * FROM t WHERE + ( 1badId",
        "SELECT * FROM t WHERE + ( CLEAR RETAINED",
        "SELECT * FROM t WHERE + [ ACCOUNT",
        "SELECT * FROM t WHERE + [ (",
        "SELECT * FROM t WHERE + [ [",
        "SELECT * FROM t WHERE + [ +",
        "SELECT * FROM t WHERE + [ 1badId",
        "SELECT * FROM t WHERE + [ CLEAR RETAINED",
        "SELECT * FROM t WHERE + $ 1badId",
        "SELECT * FROM t WHERE + + (",
        "SELECT * FROM t WHERE + + [",
        "SELECT * FROM t WHERE + + +",
        "SELECT * FROM t WHERE + + 1badId",
        "SELECT * FROM t WHERE + + CLEAR RETAINED",
        "SELECT * FROM t WHERE + 1badId ("
    };

    @Override
    public void setUp()
        throws Exception {
    }

    @Override
    public void tearDown()
        throws Exception {
    }

    @Test
    public void testParserShort() {
        for (String q : queries) {
            try {
                CompilerAPI.compile(q.toCharArray(), null /* ExecuteOptions */,
                                    metadata, null, null, null);
                //store.prepare(q);
            } catch (Throwable t) {
                // do nothing
            }
        }
    }
}
