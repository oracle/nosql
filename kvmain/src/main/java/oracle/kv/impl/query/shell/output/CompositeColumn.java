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
package oracle.kv.impl.query.shell.output;

import oracle.kv.util.shell.Column;

/**
 * CompositeColumn is used to store the nested type value, it contains 2
 * columns: label and value. For nested value, the label column is indented
 * to show the nested level.
 */
public class CompositeColumn extends Column {
    final static char DELIMITER_DEF = '|';

    private final static Align ALIGN_DEF = Align.LEFT;
    private char delimiter;
    private Column keys;
    private Column values;

    CompositeColumn() {
        this(null);
    }

    CompositeColumn(String title) {
        this(title, DELIMITER_DEF);
    }

    CompositeColumn(String title, char delimiter) {
        this(title, delimiter, ALIGN_DEF, ALIGN_DEF);
    }

    CompositeColumn(String title,  char delimiter,
                    Align keyAlign, Align valueAlign) {
        super();
        this.delimiter = delimiter;
        keys = new Column(title, keyAlign);
        values = new Column(title, valueAlign);
    }

    @Override
    public void appendTitle(String title) {
        keys.appendTitle(title);
        values.appendTitle(title);
    }

    @Override
    public void appendData(String data) {
        appendData(0, data, "");
    }

    @Override
    public void appendData(int level, String data) {
        appendData(level, data, "");
    }

    @Override
    public void appendData(int level, String key, String value) {
        keys.appendData(level, (key == null ? "" : key));
        values.appendData(value);
    }

    @Override
    public void appendSeparatorLine() {
        keys.appendSeparatorLine();
        values.appendSeparatorLine();
    }

    @Override
    public void appendEmptyLine() {
        keys.appendEmptyLine();
        values.appendEmptyLine();
    }

    @Override
    public void appendEmptyData() {
        keys.appendEmptyData();
        values.appendEmptyData();
    }

    @Override
    public int getHeight() {
        return keys.getHeight();
    }

    @Override
    public boolean isSeparatorLine(int index) {
        return keys.isSeparatorLine(index);
    }

    @Override
    public boolean isSeparatorEmptyLine(int index) {
        return keys.isSeparatorEmptyLine(index);
    }

    @Override
    public boolean isHeader(int index) {
        return keys.isHeader(index);
    }

    @Override
    public void reset(boolean keepLabelOnly, boolean keepWidth) {
        keys.reset(keepLabelOnly, keepWidth);
        values.reset(keepLabelOnly, keepWidth);
    }

    @Override
    public String getFormattedText(int index) {
        final String key = keys.getFormattedText(index);
        final String value = values.getFormattedText(index);
        final int width = keys.getWidth() + 1 + values.getWidth();
        if (isHeader(index)) {
            String title = mergeTitles(key.trim(), value.trim());
            return centerAlign(title, width);
        }

        if (key.trim().length() == 0) {
            if (keys.getIndentLevel(index) > 0) {
                return padLeft(value, width);
            }
            return padRight(value, width);
        }

        if (value.trim().length() == 0) {
            return padRight(key, width);
        }

        final char ch = isSeparatorLine(index) ?
                            keys.getSeparatorLineChar(index) : delimiter;
        return String.format("%s%s%s", key, ch, value);
    }

    private String mergeTitles(String keyTitle, String valueTitle) {
        if (keyTitle.length() == 0) {
            return valueTitle;
        }
        if (valueTitle.length() == 0) {
            return keyTitle;
        }
        if (keyTitle.equals(valueTitle)) {
            return keyTitle;
        }
        return keyTitle + " " + valueTitle;
    }
}
