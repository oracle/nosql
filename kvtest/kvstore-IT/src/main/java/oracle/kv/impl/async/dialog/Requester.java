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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import oracle.kv.impl.async.BytesUtil;
import oracle.kv.impl.async.DialogContext;
import oracle.kv.impl.async.DialogHandler;
import oracle.kv.impl.async.MessageInput;
import oracle.kv.impl.async.MessageOutput;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.CommonLoggerUtils;

public class Requester implements DialogHandler {

    private final byte[] id;
    private final byte[] request;
    private final byte[] response;
    private final Tester tester;
    private final ExecutorService executor;

    private final MessageOutput requestMessageOutput = new MessageOutput();
    private volatile DialogContext savedContext;
    private volatile boolean isDone = false;
    private AtomicReference<Throwable> errorRef = new AtomicReference<>(null);

    private byte[] actualId = null;
    private byte[] actualResp = null;

    private volatile TestHook<DialogContext> onStartHook = null;
    private volatile TestHook<DialogContext> onReadHook = null;
    private volatile TestHook<Throwable> onAbortHook = null;

    public Requester(byte[] id,
                     byte[] request,
                     byte[] response) {
        this(id, request, response, null, null);
    }

    public Requester(byte[] id,
                     byte[] request,
                     byte[] response,
                     Tester tester,
                     ExecutorService executor) {
        this.id = id;
        this.request = request;
        this.response = response;
        this.tester = tester;
        this.executor = executor;
    }

    public Requester setOnStartHook(TestHook<DialogContext> hook) {
        onStartHook = hook;
        return this;
    }

    public Requester setOnReadHook(TestHook<DialogContext> hook) {
        onReadHook = hook;
        return this;
    }

    public Requester setOnAbortHook(TestHook<Throwable> hook) {
        onAbortHook = hook;
        return this;
    }

    @Override
    public void onStart(DialogContext context, boolean aborted) {
        if (tester != null) {
            tester.semaphoreAcquire();
        }
        savedContext = context;
        if (aborted) {
            return;
        }
        if (executor == null) {
            doWriteRequest(context);
        } else {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    doWriteRequest(context);
                }
            });
        }
        assert TestHookExecute.doHookIfSet(onStartHook, context);
    }

    private void doWriteRequest(DialogContext context) {
        try {
            requestMessageOutput.writeInt(id.length);
            requestMessageOutput.write(id);
            requestMessageOutput.writeInt(request.length);
            requestMessageOutput.write(request);
            if (!context.write(requestMessageOutput, true)) {
                throw new AssertionError("Write should never fail.");
            }
            if (tester != null) {
                tester.logMesg("Wrote request: " + context);
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
        MessageInput in = null;
        try {
            in = checkNull("read result", context.read());
            int size = in.readInt();
            actualId = new byte[size];
            in.readFully(actualId);
            size = in.readInt();
            actualResp = new byte[size];
            in.readFully(actualResp);
        } catch (Throwable t) {
            if (tester != null) {
                tester.logError(t);
            } else {
                throw new Error(t);
            }
        } finally {
            isDone = true;
            assert TestHookExecute.doHookIfSet(onReadHook, context);
            if (tester != null) {
                tester.logMesg(String.format(
                            "Requester got response: " +
                            "id=%s, resp=%s, context=%s",
                            BytesUtil.toString(actualId, 0, 8),
                            BytesUtil.toString(actualResp, 0, 8),
                            context));
                tester.semaphoreRelease();
            }
            if (in != null) {
                in.discard();
            }
        }
    }

    @Override
    public void onAbort(DialogContext context, Throwable cause) {
        errorRef.set(cause);
        isDone = true;
        Error e = new Error(cause);
        assert TestHookExecute.doHookIfSet(onAbortHook, cause);
        if (tester != null) {
            tester.logError(e);
            tester.semaphoreRelease();
        } else {
            throw e;
        }
        requestMessageOutput.discard();
    }

    public void check() {
        assertEquals(
            String.format(
                "Request %s had error, context=%s, error=%s",
                new String(getId()), savedContext,
                CommonLoggerUtils.getStackTrace(errorRef.get())),
            null, errorRef.get());
        assertEquals(
            String.format(
                "Request %s is not done, context=%s",
                new String(getId()), savedContext),
            true, isDone);
        assertArrayEquals(
            String.format(
                "Id not equal to expected, " +
                "expected=%s, actual=%s, context=%s",
                BytesUtil.toString(id, 0, id.length),
                BytesUtil.toString(actualId, 0, actualId.length),
                savedContext),
            id, actualId);
        assertArrayEquals(
            String.format(
                "Request %s response not equal to expected, " +
                "expected=%s, actual=%s, context=%s",
                getId(),
                BytesUtil.toString(response, 0, response.length),
                BytesUtil.toString(actualResp, 0, actualResp.length),
                savedContext),
            response, actualResp);
    }

    public void awaitDone(long timeoutMillis) throws Exception {
        final long startTs = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTs <= timeoutMillis) {
            if (isDone) {
                return;
            }
            Thread.sleep(100);
        }
        throw new RuntimeException(
            String.format(
                "Requester %s not done, context=%s",
                new String(id), savedContext));
    }

    public boolean isDone() {
        return isDone;
    }

    public byte[] getId() {
        return id;
    }
}

