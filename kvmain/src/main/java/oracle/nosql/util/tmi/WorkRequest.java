/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 */
package oracle.nosql.util.tmi;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorCode;

/**
 * Used in defining the response payload for the REST API get-work-request and
 * list-work-requests from SC to proxy.
 *
 * It represents general work request information for DDL operation and CMEK
 * operation.
 */
public class WorkRequest {

    public enum EntityType {
        TABLE,
        CONFIGURATION
    }

    public enum OperationType {
        CREATE_TABLE,
        UPDATE_TABLE,
        DELETE_TABLE,
        UPDATE_KMS_KEY,
        REMOVE_KMS_KEY
    };

    public enum Status {
        ACCEPTED,
        IN_PROGRESS,
        FAILED,
        SUCCEEDED,
        CANCELING,
        CANCELED
    };

    public enum ActionType {
        CREATED,
        UPDATED,
        DELETED,
        IN_PROGRESS
    }

    /* The work request Id */
    private final String id;
    /* The operation type */
    private final OperationType type;
    /* The status of work request */
    private final Status status;
    /* The ocid of the compartment that contains the work request*/
    private final String compartmentId;

    /*
     * The resource affected by this work request.
     */

    /* The resource identifier */
    private final String entityId;
    /* The resource name */
    private final String entityName;
    /* The resource type */
    private final EntityType entityType;
    /* The action type */
    private final ActionType actionType;
    /* The tags of the resource */
    private final byte[] tags;

    /* The time stamp the request was created */
    private final long timeAccepted;
    /* The time stamp the request was started */
    private final long timeStarted;
    /* The time stamp the request was finished */
    private final long timeFinished;

    /* The error encountered while executing a work request */
    private final ErrorCode errorCode;
    /* The description of the issue encountered */
    private final String errorMessage;

    public WorkRequest(String id,
                       OperationType type,
                       Status status,
                       String compartmentId,
                       String entityId,
                       String entityName,
                       EntityType entityType,
                       byte[] tags,
                       ActionType actionType,
                       long timeAccepted,
                       long timeStarted,
                       long timeFinished,
                       ErrorCode errorCode,
                       String errorMessage) {
        this.id = id;
        this.type = type;
        this.status = status;
        this.compartmentId = compartmentId;

        this.entityId = entityId;
        this.entityName = entityName;
        this.entityType = entityType;
        this.actionType = actionType;
        this.tags = tags;

        this.timeAccepted = timeAccepted;
        this.timeStarted = timeStarted;
        this.timeFinished = timeFinished;

        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getId() {
        return id;
    }

    public OperationType getType() {
        return type;
    }

    public Status getStatus() {
        return status;
    }

    public String getCompartmentId() {
        return compartmentId;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getEntityName() {
        return entityName;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public byte[] getTags() {
        return tags;
    }

    public ActionType getActionType() {
        return actionType;
    }

    public long getTimeAccepted() {
        return timeAccepted;
    }

    public long getTimeStarted() {
        return timeStarted;
    }

    public long getTimeFinished() {
        return timeFinished;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return JsonUtils.print(this);
    }
 }
