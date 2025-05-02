/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * Used in definition of the JSON payloads for the REST APIs between the proxy
 * and the tenant manager.
 *
 * To serialize a Java object into a Json string:
 *   Foo foo;
 *   String jsonPayload = objectMapper.writeValueAsString(foo);
 *
 * To deserialize a Json string into this object:
 *   Foo foo = objectMapper.readValue(<jsonstring>, Foo.class);
 *
 * This class is also used within the proxy to ferry table related info.
 *
 * This class encapsulates the REST response to the path:
 *   /tables/{tablename}/history
 * See TMService.yaml.
 *
 * TODO: flesh this out.
 */
public class TableHistoryInfo {

    @Override
    public String toString() {
        // TODO
        return "TableHistory: {}";
    }
}
