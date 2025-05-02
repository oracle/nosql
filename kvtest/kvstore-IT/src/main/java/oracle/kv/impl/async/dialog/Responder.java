/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async.dialog;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;

public class Responder implements DialogHandler {

    private final Tester tester;
    private final byte[] request;
    private final byte[] response;

    private volatile DialogContext savedContext;
    private volatile boolean isDone = false;
    private volatile boolean hasError = false;

    private byte[] actualId = null;
    private byte[] actualReq = null;

    public Responder(byte[] request, byte[] response) {
        this(request, response, null);
    }

    public Responder(byte[] request,
                     byte[] response,
                     Tester tester) {
        this.request = request;
        this.response = response;
        this.tester = tester;
    }

    @Override
    public void onStart(DialogContext context, boolean aborted) {
        if (tester != null) {
            tester.semaphoreAcquire();
        }
        savedContext = context;
        if (tester != null) {
            tester.logMesg("Done onStart: " + context);
        }
    }

    @Override
    public void onCanWrite(DialogContext context) {
        Error e = new Error("should not be in onCanWrite");
        if (tester != null) {
            tester.logError(e);
        } else {
            throw e;
        }
    }

    @Override
    public void onCanRead(DialogContext context, boolean finished) {
        try {
            if (!finished) {
                throw new AssertionError(
                        "Expected request to be finished");
            }
            final MessageInput in = checkNull("read result", context.read());
            int size = in.readInt();
            actualId = new byte[size];
            in.readFully(actualId);
            size = in.readInt();
            actualReq = new byte[size];
            in.readFully(actualReq);
            final MessageOutput out = new MessageOutput();
            out.writeInt(actualId.length);
            out.write(actualId);
            out.writeInt(response.length);
            out.write(response);
            if (!context.write(out, true)) {
                throw new AssertionError(
                        "Write should never fail for single response");
            }
            isDone = true;
            if (tester != null) {
                tester.logMesg(
                    String.format(
                        "Responder got request: id=%s, req=%s, context=%s",
                        BytesUtil.toString(actualId, 0, 8),
                        BytesUtil.toString(actualReq, 0, 8),
                        context));
                tester.semaphoreRelease();
            }
        } catch (Throwable t) {
            if (tester != null) {
                tester.logError(t);
            } else {
                throw new Error(t);
            }
        }
    }

    @Override
    public void onAbort(DialogContext context, Throwable cause) {
        hasError = true;
        Error e = new Error(cause);
        if (tester != null) {
            tester.logError(e);
            tester.semaphoreRelease();
        } else {
            throw e;
        }
    }

    public void check() {
        assertEquals("Context had error: " + savedContext, false, hasError);
        assertEquals("Context is not done: " + savedContext, true, isDone);
        assertArrayEquals(String.format(
                              "Request not equal to expected, " +
                              "expected=%s, actual=%s, context=%s",
                              BytesUtil.toString(request, 0, request.length),
                              BytesUtil.toString(actualReq, 0, actualReq.length),
                              savedContext),
                          request, actualReq);
    }
}

