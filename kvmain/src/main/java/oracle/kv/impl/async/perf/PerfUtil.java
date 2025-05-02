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
 */

package oracle.kv.impl.async.perf;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Stack;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import oracle.kv.impl.async.perf.JsonReference.JsonKey;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * Common utilities for collecting and processing async perf.
 */
public class PerfUtil {

    /**
     * Returns the the maximum power of 2 less than or equal to the given
     * {@code sampleRate}.
     *
     * <p>A sample rate of less or equal than 0 is treated as Long.MAX_VALUE.
     */
    public static long getSampleRateHighestOneBit(long sampleRate) {
        return (sampleRate <= 0) ?
            Long.highestOneBit(Long.MAX_VALUE) :
            Long.highestOneBit(sampleRate);
    }

    /**
     * Returns true if the specified count represents an event that should be
     * sampled under the powerOfTwoSampleRate. The {@code count} represents the
     * sequence order of an event. The {@code powerOfTwoSampleRate} represents
     * a sample rate and is adjusted to be always a power of two. The event
     * will be sampled if the {@code count} is a multiple of {@code
     * powerOfTwoSampleRate} (including zero). Or in another word, one event
     * will be sampled for every {@code powerOfTwoSampleRate} events occurred.
     */
    public static boolean shouldSample(long count, long powerOfTwoSampleRate) {
        return ((count & (powerOfTwoSampleRate - 1)) == 0);
    }

    /**
     * Summarize a stream of long values.
     */
    public static ObjectNode summarize(LongStream stream) {
        final long[] values = stream.sorted().toArray();
        final ObjectNode result = JsonUtils.createObjectNode();
        if (values.length == 0) {
            return result;
        }
        final int n = values.length;
        final int half = (n % 2 == 0) ? n / 2 - 1 : n / 2;
        result.put("min", values[0]);
        result.put("avg", Arrays.stream(values).average().orElse(0));
        result.put("med", values[half]);
        result.put("max", values[n - 1]);
        return result;
    }

    /**
     * Summarize a stream of double values.
     */
    public static ObjectNode summarize(DoubleStream stream) {
        final double[] values = stream.sorted().toArray();
        final ObjectNode result = JsonUtils.createObjectNode();
        if (values.length == 0) {
            return result;
        }
        final int n = values.length;
        final int half = (n % 2 == 0) ? n / 2 - 1 : n / 2;
        result.put("min", values[0]);
        result.put("avg", Arrays.stream(values).average().orElse(0));
        result.put("med", values[half]);
        result.put("max", values[n - 1]);
        return result;
    }

    /**
     * Adds the summry to a payload.
     */
    public static void addSummary(ObjectNode payload,
                                  String name,
                                  LongStream stream) {
        final ObjectNode summary = summarize(stream);
        if (!summary.isEmpty()) {
            payload.put(name, summary);
        }
    }

    /**
     * Adds the summry to a payload.
     */
    public static void addSummary(ObjectNode payload,
                                  String name,
                                  DoubleStream stream) {
        final ObjectNode summary = summarize(stream);
        if (!summary.isEmpty()) {
            payload.put(name, summary);
        }
    }

    /**
     * Returns an iterator of all the descendent {@link JsonNode} with depth
     * first search and yields reference-value pairs.
     */
    public static Iterator<Map.Entry<JsonReference, JsonNode>>
        depthFirstSearchIterator(JsonNode root)
    {
        return new DepthFirstSearchIterator(root);
    }

    private static class DepthFirstSearchIterator extends SearchIterator {

        private final Stack<Map.Entry<JsonReference, JsonNode>> stack =
            new Stack<>();

        private DepthFirstSearchIterator(JsonNode root) {
            stack.add(new SimpleImmutableEntry<>(
                JsonReference.ROOT, root));
        }

        @Override
        public boolean hasNext() {
            return !stack.isEmpty();
        }

        @Override
        protected Entry<JsonReference, JsonNode> getNextEntry() {
            return stack.pop();
        }

        @Override
        protected void populate(Entry<JsonReference, JsonNode> child) {
            stack.add(child);
        }

        @Override
        protected boolean reverseOrderArrayElements() {
            return true;
        }
    }

    private static abstract class SearchIterator
        implements Iterator<Map.Entry<JsonReference, JsonNode>> {

        @Override
        public Map.Entry<JsonReference, JsonNode> next() {
            /* Pops the next entry which is on the top of the stack. */
            final Map.Entry<JsonReference, JsonNode> nextEntry =
                getNextEntry();
            /*
             * Prepares for the stack so that the top of it is the entry to
             * yield after nextEntry.
             */
            final JsonReference nextRef = nextEntry.getKey();
            final JsonNode nextNode = nextEntry.getValue();
            /*
             * If the nextNode is a primitive node, nothing to do to populate
             * the stack.
             */
            if (nextNode.isPrimitive()) {
                return nextEntry;
            }
            /*
             * Populates the stack with the child elements of array or object
             * nextNode.
             */
            final List<JsonKey<?>> keys = new ArrayList<>(nextRef.getKeys());
            if (nextNode.isArray()) {
                final ArrayNode array = (ArrayNode) nextNode;
                final int size = array.size();
                final IntStream indexStream =
                    reverseOrderArrayElements()
                    ? IntStream.range(0, size).map((i) -> size - i - 1)
                    : IntStream.range(0, size);
                indexStream.forEach((i) -> {
                    final JsonNode child = array.get(i);
                    keys.add(new JsonReference.ArrayKey(i));
                    populate(new SimpleImmutableEntry<>(
                        new JsonReference(keys), child));
                    keys.remove(keys.size() - 1);
                });
                return nextEntry;
            }
            assert nextNode.isObject();
            final ObjectNode object = (ObjectNode) nextNode;
            object.entrySet().forEach((childEntry) -> {
                final String childKey = childEntry.getKey();
                final JsonNode child = childEntry.getValue();
                keys.add(new JsonReference.ObjectKey(childKey));
                populate(new SimpleImmutableEntry<>(
                    new JsonReference(keys), child));
                keys.remove(keys.size() - 1);
            });
            return nextEntry;
        }

        /** Returns the next entry for the iterator.  */
        protected abstract Map.Entry<JsonReference, JsonNode> getNextEntry();

        /** Populates a child entry of the next entry.  */
        protected abstract
            void populate(Map.Entry<JsonReference, JsonNode> child);

        /**
         * Returns {@code true} if adding the array elements by reverse order
         * when populating the data structure during search. This is needed for
         * the two types of searches (depth first search and breath first
         * search) to yield array elements in the order consistent with the
         * array order semantics. During depth first search, the elements are
         * yielded in FILO order: the elements visited first are yielded last.
         * During breath first search, however, it is FIFO.
         */
        protected abstract boolean reverseOrderArrayElements();
    }

    /**
     * Returns an iterator of all the descendent {@link JsonNode} with breadth
     * first search and yields reference-value pairs.
     */
    public static Iterator<Map.Entry<JsonReference, JsonNode>>
        breadthFirstSearchIterator(JsonNode root)
    {
        return new BreadthFirstSearchIterator(root);
    }

    private static class BreadthFirstSearchIterator extends SearchIterator {

        private final Queue<Map.Entry<JsonReference, JsonNode>> queue =
            new ArrayDeque<>();

        private BreadthFirstSearchIterator(JsonNode root) {
            queue.add(new SimpleImmutableEntry<>(
                JsonReference.ROOT, root));
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        protected Entry<JsonReference, JsonNode> getNextEntry() {
            return queue.poll();
        }

        @Override
        protected void populate(Entry<JsonReference, JsonNode> child) {
            queue.add(child);
        }

        @Override
        protected boolean reverseOrderArrayElements() {
            return false;
        }
    }

}
