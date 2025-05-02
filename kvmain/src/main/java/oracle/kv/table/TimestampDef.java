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

package oracle.kv.table;

/**
 * TimestampDef is an extension of {@link FieldDef} to encapsulate a Timestamp
 * type.
 *
 * @since 4.3
 */
public interface TimestampDef extends FieldDef {

    /**
     * <p>
     * The default pattern of Timestamp string, it is the pattern used to format
     * a TimestampValue to a string or parse TimestampValue from a string, the
     * time zone for formatting and parsing is UTC zone.
     * </p>
     * <p>
     * About the number of digits of fractional second:
     * </p>
     * <ul>
     *  <li>When format a TimestampValue to a string, the number of digits is
     *  determined by the precision of TimestampDef.
     *  <pre>
     *  e.g. A table t1 contains 3 TIMESTAMP fields:
     *      CREATE TABLE t1(id INTEGER,
     *                      ts0 TIMESTAMP(0),
     *                      ts3 TIMESTAMP(3),
     *                      ts9 TIMESTAMP(9),
     *                      PRIMARY KEY(id));
     *
     *  Table t1 = store.getTableAPI().getTable("t1");
     *
     *  TimestampDef def0 = t1.getField("ts0").asTimestamp();
     *  TimestampDef def3 = t1.getField("ts3").asTimestamp();
     *  TimestampDef def9 = t1.getField("ts9").asTimestamp();
     *
     *  /* Supposes local zone is UTC. *&#47;
     *  Timestamp ts = Timestamp.valueOf("2016-07-27 12:45:21.123456789");
     *
     *  /* Converts the TimestampValue created with def0 to a string. *&#47;
     *  String str0 = def0.createTimestamp(ts).toString();
     *  System.out.println(str0);       // 2016-07-27T12:45:21
     *
     *  /* Converts the TimestampValue created with def3 to a string. *&#47;
     *  String str3 = def3.createTimestamp(ts).toString();
     *  System.out.println(str3);       // 2016-07-27T12:45:21.123
     *
     *  /* Converts the TimestampValue created with def9 to a string. *&#47;
     *  String str9 = def9.createTimestamp(ts).toString();
     *  System.out.println(str9);       // 2016-07-27T12:45:21.123456789
     *  </pre>
     *  </li>
     *  <li>When parse a TimestampValue from a string, the fractional second
     *  part is optional, the number of digits can any value between 0 and 9.
     *  <pre>
     *  String str = "2016-07-27T12:45:21.987654321";
     *  /*
     *   * The value of tsv0 is "2016-07-27 12:45:22 UTC", the fractional
     *   * second is rounded up to 1 second.
     *   *&#47;
     *  TimestampValue tsv0 = def0.fromString(str);
     *
     *  /*
     *   * The value of tsv3 is 2016-07-27 12:45:21.988 UTC, the fractional
     *   * second is rounded up to 988 with precision of 3.
     *   *&#47;
     *  TimestampValue tsv3 = def3.fromString(str);
     *
     *  /* The value of tsv9 is 2016-07-27 12:45:21.987654321 UTC. *&#47;
     *  TimestampValue tsv9 = def9.fromString(str);
     *  </pre>
     *  </li>
     * </ul>
     */
    public static final String DEFAULT_PATTERN = "uuuu-MM-dd['T'HH:mm:ss]";

    /**
     * @return the precision of fractional second
     */
    public int getPrecision();

    /**
     * @return a deep copy of this object
     */
    @Override
    public TimestampDef clone();

    /**
     * Creates a TimestampValue instance from a String, the string must be in
     * the format of {@code DEFAULT_PATTERN} with UTC zone.
     *
     * @param timestampString a string in format of default pattern.
     *
     * @return a TimestampValue instance.
     *
     * @throws IllegalArgumentException if the string cannot be parsed with the
     * default pattern correctly.
     */
    public TimestampValue fromString(String timestampString);

    /**
     * Creates a TimestampValue instance from a String with specified pattern.
     *
     * @param timestampString a timestamp string in format of specified
     * {@code pattern} or default pattern {@link TimestampDef#DEFAULT_PATTERN}
     * if {@code pattern} is null.
     * @param pattern the pattern for the timestampString. If null, then default
     * pattern {@link TimestampDef#DEFAULT_PATTERN} is used to parse the string.
     * The symbols that can be used to specify a pattern are described in
     * {@link java.time.format.DateTimeFormatter}.
     * @param withZoneUTC true if UTC time zone is used as default zone when
     * parse from a timestamp string, otherwise local time zone is used as
     * default zone.  If the timestampString has zone information, then the
     * zone will take precedence over the default zone.
     *
     * @return a TimestampValue instance
     *
     * @throws IllegalArgumentException if the string cannot be parsed with the
     * the given pattern correctly.
     */
    public TimestampValue fromString(String timestampString,
                                     String pattern,
                                     boolean withZoneUTC);

    /**
     * Creates a TimestampValue instant set to the current system millisecond
     * time.
     *
     * @return a TimestampValue instance
     */
    public TimestampValue currentTimestamp();
}
