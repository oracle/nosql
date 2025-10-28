/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.UUID;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.TimestampValue;

import org.junit.Test;

/**
 * Test I18N in the cloud driver. This test and associated data files were
 * supplied by the i18n group.
 */
public class ProxyI18NTest extends ProxyTestBase {
    private static final String basePath =
        getProxyBase() + "/oracle/nosql/proxy/";
    private static final String[] Jsonfilesname = {
    	"utf8.json","utf8bom.json",
    	"utf16le.json",	"utf16lebom.json",
    	"utf16be.json","utf16bebom.json"
    };

    private static final String expfile = "utf8_testdata.txt";
    private static final String outputres = "results_testdata.txt";
    private static final String jsondata = "utf8_jsondata.txt";
    private String expstr = null;

    /*
     * Test creation from various json encodings
     */
    @Test
    public void createJsonTest(){
        try {
            for (String element : Jsonfilesname) {
                File file = new File(basePath + element);
                FileInputStream fis = new FileInputStream(file);
                MapValue mv =
                    FieldValue.createFromJson(fis, new JsonOptions()).asMap();
                expstr = readexpfile("fr",1);
                assertEquals(expstr, mv.getString("name"));
                fis.close();
            }
        } catch (Exception e) {
            fail("Exception: " + e);
        }
    }

    /*
     * Test French input
     */
    @Test
    public void fr_simpleTest() throws Exception {

        try {
            /*
             * Create a simple table with an integer key and a single
             * name field
             */
            TableResult tres = tableOperation(
                handle,
                "create table if not exists users(id integer, " +
                "name string, primary key(id))",
                new TableLimits(500, 500, 50),
                TableResult.State.ACTIVE,
                20000);
            assertEquals(TableResult.State.ACTIVE, tres.getTableState());

            /*  PUT a row    */

            /* construct a simple row */
            MapValue value = new MapValue().put("id", 1).
                put("name", "\u00E7\u00E9_myname");

            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName("users");

            PutResult putRes = handle.put(putRequest);
            assertNotNull("Put failed", putRes.getVersion());
            assertWriteKB(putRes);

            /*   GET the row      */
            MapValue key = new MapValue().put("id", 1);
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName("users");

            GetResult getRes = handle.get(getRequest);

            String getstr = getRes.getValue().toString();

            expstr = readexpfile("fr",2);

            assertEquals(expstr, getstr);

            /* PUT a row using JSON  */

            /* construct a simple row */
            String jsonString = readexpfile("fr",3);
            putRequest = new PutRequest()
                .setValueFromJson(jsonString, null) // no options
                .setTableName("users");

            putRes = handle.put(putRequest);

            /*   GET the new row  */
            key = new MapValue().put("id", 2);
            getRequest = new GetRequest()
                .setKey(key)
                .setTableName("users");

            getRes = handle.get(getRequest);

            expstr = readexpfile("fr",3);
            getstr = getRes.getValue().toString();

            assertEquals(expstr, getstr);

            /* DELETE a row  */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName("users");

            handle.delete(delRequest);
        } catch (NoSQLException nse) {
            System.err.println("Op failed: " + nse.getMessage());
        } catch (Exception e) {
            System.err.println("Exception processing msg: " + e);
            e.printStackTrace();
        }
    }

    @Test
    public void jsontableTest(){

    	/* Create a table */
    	TableResult tres = tableOperation(
            handle,
            "create table if not exists restaurants(uid string, " +
            "restaurantJSON JSON, primary key(uid))",
            new TableLimits(500, 500, 50),
            TableResult.State.ACTIVE,
            20000);

        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* Create an index */
        tres = tableOperation(
            handle,
            "CREATE INDEX IF NOT EXISTS idx_json_name on restaurants " +
            " (restaurantJSON.name as string)",
            null,
            TableResult.State.ACTIVE,
            20000);
        assertEquals(TableResult.State.ACTIVE, tres.getTableState());

        /* populate jason data to table */
        BufferedReader br = null;
        FileReader fr = null;

        try {
            String jsonString = "";
            String currLine;
            int pCount = 0;
            boolean buildObj = false;
            boolean beganParsing = false;
            String jsonfilePath = basePath + jsondata;

            fr = new FileReader(jsonfilePath);
            br = new BufferedReader(fr);


            /*
             * Parse the sample JSON file to find the matching parenthesis to
             * construct JSON string
             */
            while ((currLine = br.readLine()) != null) {
                pCount += countParens(currLine, '{');

                // Empty line in the data file
                if (currLine.length() == 0) {
                    continue;
                }

                // Comments must start at column 0 in the
                // data file.
                if (currLine.charAt(0) == '#') {
                    continue;
                }

                // If we've found at least one open paren, it's time to
                // start collecting data
                if (pCount > 0) {
                    buildObj = true;
                    beganParsing = true;
                }

                if (buildObj) {
                    jsonString += currLine;
                }

                /*
                 * If our open and closing parens balance (the count
                 * is zero) then we've collected an entire object
                 */
                pCount -= countParens(currLine, '}');
                if (pCount < 1)
                    {
                        buildObj = false;
                        /*
                         * If we started parsing data, but buildObj is false
                         * then that means we've reached the end of a JSON
                         * object in the input file. So write the object
                         * to the table, using the PutRequest
                         */
                    }

                if (beganParsing && !buildObj) {

                    /*
                     * Use the putFromJSON to automatically convert JSON string
                     * into JSON object
                     */
                    MapValue value = new MapValue().put("uid", generateUUID()).
                        putFromJson("restaurantJSON", jsonString, null);
                    PutRequest putRequest = new PutRequest().setValue(value).
                        setTableName("restaurants");
                    PutResult putRes = handle.put(putRequest);
                    assertNotNull(putRes.getVersion());
                    jsonString = "";
                }
            }

            /* query json table */
            /* index line 4 name in expfile */
            String idxname = readexpfile("fr", 4 );
            String predQuery = "SELECT * FROM restaurants r WHERE " +
                " r.restaurantJSON.name = \"" + idxname + "\"";
            //String predQuery = "SELECT * FROM restaurants r WHERE " +
            //" r.restaurantJSON.name < \"" + idxname + "\"";

            //Create the Query Request
            QueryRequest queryRequest = new QueryRequest().
                setStatement(predQuery);

            //Execute the query and get the response
            QueryResult queryRes = handle.query(queryRequest);
            if (queryRes.getResults().size() >0) {
                String name;
                String address;
                String phonenumber;
                String mobile_reserve_url;

                for (MapValue record : queryRes.getResults()) {
                    MapValue jsonValue = record.get("restaurantJSON").asMap();

                    name = jsonValue.getString("name");
                    address = jsonValue.getString("address");
                    phonenumber = jsonValue.getString("phone");
                    mobile_reserve_url =
                        jsonValue.getString("mobile_reserve_url");

                    // write the result data to an outputfile
                    String oputrespath = basePath + outputres;
                    OutputStreamWriter pw = null;
                    FileOutputStream fs =
                        new FileOutputStream(oputrespath,true);
                    pw = new OutputStreamWriter(fs,"UTF8");
                    pw.write(name + "\t");
                    pw.write(address + "\t");
                    pw.write(phonenumber + "\t");
                    pw.write(mobile_reserve_url + "\n");
                    pw.close();
                    /* delete output file */
                    new File(oputrespath).delete();
                }
            }
        } catch (FileNotFoundException fnfe) {
            fail(" File not found: " + fnfe );
        } catch (IOException ioe) {
            fail("IOException: " + ioe );
            System.exit(-1);
        } catch (NoSQLException nse) {
            fail("jsontableTest Op failed: " + nse.getMessage());
        } catch (Exception e) {
            fail( "Exception processing msg: " + e );
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
                if (fr != null) {
                    fr.close();
                }
            } catch (IOException iox) {
                // ignore
            }
        }
    }

    @Test
    public void timestampTest(){
        String timesp1 = "1970-01-01T00:00:00Z";
        String timesp2 = "1970-01-01T00:00:00+00:00";
        timestamp(timesp1);
        timestamp(timesp2);
   }

    /*
     * Used by populateTable() to know when a JSON object
     * begins and ends in the input data file.
     */
    private static int countParens(String line, char p) {
        int c = 0;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == p) {
                c++;
            }
        }
        return c;
    }

    private static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static String readexpfile(String lang, int line){
        BufferedReader br = null;
        InputStreamReader isr = null;
        String currLine;
        String expstr = null;
        String langfg = lang+"{";

        try {
            //Get the expected data file
            File file = new File(basePath + expfile);

            //Read file content using utf8
            isr = new InputStreamReader(new FileInputStream(file), "utf8");

            br  = new BufferedReader(isr);

            while ((currLine = br.readLine()) != null) {

                // Empty line in the data file
                if (currLine.length() == 0) {
                    continue;
                }

                // Comments must start at column 0 in the data file.
                if (currLine.charAt(0) == '#') {
                    continue;
                }

                /*
                 * If we've found the expected language, it's time to start
                 * collecting data
                 */
                if (currLine.equals(langfg)) {
                    int i = 0;
                    while (i != line){
                        i++ ;
                        currLine = br.readLine();
                    }
                    String currstr = currLine.substring(currLine.indexOf(":")+1);
                    expstr = currstr.trim();
                    break;
                }
            }
        } catch (Exception e) {
            fail("Exception: " + e);
        } finally {
            try {
                if (isr != null) {
                    isr.close();
                }
                if (br != null) {
                    br.close();
                }
            } catch (IOException ioe) {
                // ignore
            }
        }

        return expstr;
    }

    /* Used by timestamp test is enabled */
    private  static void timestamp(String s) {
        try {
            @SuppressWarnings("unused")
            TimestampValue v = new TimestampValue(s);
            // System.out.println(s +" = long(" + v.getLong() + ")");
        } catch (Exception e) {
            fail("test timestamp failed: " + e.getMessage());
        }
    }
}
