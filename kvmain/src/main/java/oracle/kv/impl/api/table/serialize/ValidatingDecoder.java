/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oracle.kv.impl.api.table.serialize;

import java.io.IOException;
import java.nio.ByteBuffer;

import oracle.kv.impl.api.table.serialize.parsing.Symbol;
import oracle.kv.impl.api.table.serialize.util.Utf8;

/**
 * An implementation of {@link Decoder} that ensures that the sequence of
 * operations conforms to a schema.
 * ValidatingDecoder is not thread-safe.
 *
 * @see Decoder
 * @see DecoderFactory
 */
public class ValidatingDecoder extends ParsingDecoder {
    protected Decoder in;

    ValidatingDecoder(Symbol root, Decoder in) {
        super(root);
        this.configure(in);
    }

    /** Re-initialize, reading from a new underlying Decoder. */
    public ValidatingDecoder configure(Decoder decoder) {
        this.parser.reset();
        this.in = decoder;
        return this;
    }

    @Override
    public void readNull() throws IOException {
        parser.advance(Symbol.NULL);
        in.readNull();
    }

    @Override
    public boolean readBoolean() throws IOException {
        parser.advance(Symbol.BOOLEAN);
        return in.readBoolean();
    }

    @Override
    public int readInt() throws IOException {
        parser.advance(Symbol.INT);
        return in.readInt();
    }

    @Override
    public long readLong() throws IOException {
        parser.advance(Symbol.LONG);
        return in.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        parser.advance(Symbol.FLOAT);
        return in.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        parser.advance(Symbol.DOUBLE);
        return in.readDouble();
    }

    @Override
    public Utf8 readString(Utf8 old) throws IOException {
        parser.advance(Symbol.STRING);
        return in.readString(old);
    }

    @Override
    public String readString() throws IOException {
        parser.advance(Symbol.STRING);
        return in.readString();
    }

    @Override
    public void skipString() throws IOException {
        parser.advance(Symbol.STRING);
        in.skipString();
    }

    @Override
    public ByteBuffer readBytes(ByteBuffer old) throws IOException {
        parser.advance(Symbol.BYTES);
        return in.readBytes(old);
    }

    @Override
    public void skipBytes() throws IOException {
        parser.advance(Symbol.BYTES);
        in.skipBytes();
    }

    private void checkFixed(int size) throws IOException {
        parser.advance(Symbol.FIXED);
        Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
        if (size != top.size) {
            throw new SerializationRuntimeException(
                    "Incorrect length for fixed binary: expected " + top.size
                            + " but received " + size + " bytes.");
        }
    }

    @Override
    public void readFixed(byte[] bytes, int start, int len) throws IOException {
        checkFixed(len);
        in.readFixed(bytes, start, len);
    }

    @Override
    public void skipFixed(int length) throws IOException {
        checkFixed(length);
        in.skipFixed(length);
    }

    @Override
    protected void skipFixed() throws IOException {
        parser.advance(Symbol.FIXED);
        Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
        in.skipFixed(top.size);
    }

    @Override
    public int readEnum() throws IOException {
        parser.advance(Symbol.ENUM);
        Symbol.IntCheckAction top = (Symbol.IntCheckAction) parser.popSymbol();
        int result = in.readEnum();
        if (result < 0 || result >= top.size) {
            throw new SerializationRuntimeException(
                    "Enumeration out of range: max is " + top.size
                            + " but received " + result);
        }
        return result;
    }

    @Override
    public long readArrayStart() throws IOException {
        parser.advance(Symbol.ARRAY_START);
        long result = in.readArrayStart();
        if (result == 0) {
            parser.advance(Symbol.ARRAY_END);
        }
        return result;
    }

    @Override
    public long arrayNext() throws IOException {
        parser.processTrailingImplicitActions();
        long result = in.arrayNext();
        if (result == 0) {
            parser.advance(Symbol.ARRAY_END);
        }
        return result;
    }

    @Override
    public long skipArray() throws IOException {
        parser.advance(Symbol.ARRAY_START);
        for (long c = in.skipArray(); c != 0; c = in.skipArray()) {
            while (c-- > 0) {
                parser.skipRepeater();
            }
        }
        parser.advance(Symbol.ARRAY_END);
        return 0;
    }

    @Override
    public long readMapStart() throws IOException {
        parser.advance(Symbol.MAP_START);
        long result = in.readMapStart();
        if (result == 0) {
            parser.advance(Symbol.MAP_END);
        }
        return result;
    }

    @Override
    public long mapNext() throws IOException {
        parser.processTrailingImplicitActions();
        long result = in.mapNext();
        if (result == 0) {
            parser.advance(Symbol.MAP_END);
        }
        return result;
    }

    @Override
    public long skipMap() throws IOException {
        parser.advance(Symbol.MAP_START);
        for (long c = in.skipMap(); c != 0; c = in.skipMap()) {
            while (c-- > 0) {
                parser.skipRepeater();
            }
        }
        parser.advance(Symbol.MAP_END);
        return 0;
    }

    @Override
    public int readIndex() throws IOException {
        parser.advance(Symbol.UNION);
        Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
        int result = in.readIndex();
        parser.pushSymbol(top.getSymbol(result));
        return result;
    }

    @Override
    public Symbol doAction(Symbol input, Symbol top) throws IOException {
        return null;
    }

    @Override
    public long readCRDTStart() throws IOException {
        parser.advance(Symbol.CRDT_START);
        long result = in.readCRDTStart();
        if (result == 0) {
            parser.advance(Symbol.CRDT_END);
        }
        return result;
    }

    @Override
    public long CRDTNext() throws IOException {
        parser.processTrailingImplicitActions();
        long result = in.CRDTNext();
        if (result == 0) {
            parser.advance(Symbol.CRDT_END);
        }
        return result;
    }

    @Override
    public long skipCRDT() throws IOException {
        parser.advance(Symbol.CRDT_START);
        for (long c = in.skipCRDT(); c != 0; c = in.skipCRDT()) {
            while (c-- > 0) {
                parser.skipRepeater();
            }
        }
        parser.advance(Symbol.CRDT_END);
        return 0;
    }
}
