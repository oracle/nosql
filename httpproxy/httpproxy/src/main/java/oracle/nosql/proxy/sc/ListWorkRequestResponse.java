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

package oracle.nosql.proxy.sc;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.WorkRequest;

/**
 * Response to a TenantManager listWorkRequests operation.
 */
public class ListWorkRequestResponse extends CommonResponse {
   private final WorkRequest[] workRequests;
   private final String nextPageToken;

   public ListWorkRequestResponse(int httpResponse,
                                  WorkRequest[] workRequests,
                                  String nextPageToken) {
       super(httpResponse);
       this.workRequests = workRequests;
       this.nextPageToken = nextPageToken;
   }

   public ListWorkRequestResponse(ErrorResponse err) {
       super(err);
       workRequests = null;
       nextPageToken = null;
   }

   /**
    * Returns an array of WorkRequest, or null on failure
    */
   public WorkRequest[] getWorkRequests() {
       return workRequests;
   }

   /**
    * Returns the starting point for retrieving next batch of results, or null
    * on failure
    */
   public String getNextPageToken() {
       return nextPageToken;
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
           sb.append(JsonUtils.prettyPrint(workRequests)).append(",");
           sb.append("\"nextPageToken\": ")
             .append(nextPageToken)
             .append("}");
           return sb.toString();
       } catch (IllegalArgumentException iae) {
           return ("Error serializing payload: " + iae.getMessage());
       }
   }

   @Override
   public String toString() {
       StringBuilder sb = new StringBuilder();
       sb.append("ListWorkRequestResponse [workRequests=[");
       if (workRequests != null) {
           for (int i = 0; i < workRequests.length; i++) {
               sb.append(workRequests[i].toString());
               if (i < (workRequests.length - 1)) {
                   sb.append(",");
               }
           }
       }
       sb.append("], nextPageToken=")
         .append(nextPageToken)
         .append("]");
       return sb.toString();
   }
}
