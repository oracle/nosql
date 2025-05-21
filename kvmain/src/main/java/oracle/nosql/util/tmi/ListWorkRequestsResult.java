/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

/**
 * Used in defining the response payload for the REST API list-work-requests
 * from SC to proxy.
 */
public class ListWorkRequestsResult {
    /* The array of WorkRequests  */
    private final WorkRequest[] workRequests;
    /*
     * The page token represents the starting point for retrieving next batch
     * of results.
     */
    private final String nextPageToken;

    public ListWorkRequestsResult(WorkRequest[] requests,
                                  String nextPageToken) {
        this.workRequests = requests;
        this.nextPageToken = nextPageToken;
    }

    public WorkRequest[] getWorkRequests() {
        return workRequests;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
}
