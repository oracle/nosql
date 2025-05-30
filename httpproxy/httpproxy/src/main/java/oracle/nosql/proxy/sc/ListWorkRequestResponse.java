/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.sc;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.DdlHistoryEntry;

/**
 * Response to a TenantManager listWorkRequests operation.
 */
public class ListWorkRequestResponse extends CommonResponse {
   private final DdlHistoryEntry[] workRequestInfos;
   private final int lastIndexReturned;

   public ListWorkRequestResponse(int httpResponse,
                                  DdlHistoryEntry[] workRequestInfos,
                                  int lastIndexReturned) {
       super(httpResponse);
       this.workRequestInfos = workRequestInfos;
       this.lastIndexReturned = lastIndexReturned;
   }

   public ListWorkRequestResponse(ErrorResponse err) {
       super(err);
       workRequestInfos = null;
       lastIndexReturned = 0;
   }

   /**
    * Returns a list of DdlHistoryEntry, or null on failure
    */
   public DdlHistoryEntry[] getWorkRequestInfos() {
       return workRequestInfos;
   }

   public int getLastIndexReturned() {
       return lastIndexReturned;
   }

   /**
    * {
    *   "workRequests" : [...],
    *   "lastIndex" : 5
    * }
    */
   @Override
   public String successPayload() {
       try {
           StringBuilder sb = new StringBuilder();
           sb.append("{\"workRequests\": ");
           sb.append(JsonUtils.prettyPrint(workRequestInfos)).append(",");
           sb.append("\"lastIndex\": ").append(lastIndexReturned).append("}");
           return sb.toString();
       } catch (IllegalArgumentException iae) {
           return ("Error serializing payload: " + iae.getMessage());
       }
   }

   @Override
   public String toString() {
       StringBuilder sb = new StringBuilder();
       sb.append("ListWorkRequestResponse [tableInfos=[");
       if (workRequestInfos == null) {
           sb.append("null");
       } else {
           for (int i = 0; i < workRequestInfos.length; i++) {
               sb.append(workRequestInfos[i].toString());
               if (i < (workRequestInfos.length - 1)) {
                   sb.append(",");
               }
           }
       }
       sb.append("]]");
       return sb.toString();
   }
}
