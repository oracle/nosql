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
package oracle.kv.util.shell;

import java.util.ArrayList;
import java.util.List;

/**
 * Class Column is to store a column of elements of a table, its element is
 * represented by a Cell class.
 */
public class Column {
    public static enum Align {LEFT, RIGHT, CENTER, UNALIGNED}

    /* Default character for separator */
    private final static char SEPARATOR_DEF = '-';

    /* The default number of white spaces added before text in a cell */
    private final static int LEFT_PAD_DEF = 1;

    /* The default number of white spaces added after text in a cell */
    private final static int RIGHT_PAD_DEF = 1;

    /*
     * The default number of indentation spaces to display complex value
     * hierarchically.
     */
    private final static int INDENT_DEF = 4;

    private final static char WHITESPACE = ' ';

    private final int leftPad;
    private final int rightPad;
    private final Align alignment;
    private final int indent;

    private List<Cell> cells;
    private int width;
    private int labelWidth;
    private int labelSize;

    public Column() {
        this(null);
    }

    public Column(String title) {
        this(title, Align.LEFT);
    }

    public Column(String title, Align alignment) {
        this(title, alignment, true, LEFT_PAD_DEF, RIGHT_PAD_DEF);
    }

    public Column(String title,
                  Align alignment,
                  boolean hasTopBorder,
                  int leftPad,
                  int rightPad) {
        this(title, alignment, hasTopBorder, leftPad, rightPad, INDENT_DEF);
    }

    public Column(String title,
                  Align alignment,
                  boolean hasTopBorder,
                  int leftPad,
                  int rightPad,
                  int indent) {

        cells = new ArrayList<Cell>();
        width = labelWidth = 0;
        if (title != null) {
            if (hasTopBorder) {
                appendSeparatorLine();
            }
            appendTitle(title);
        }
        this.alignment = alignment;
        this.leftPad = leftPad;
        this.rightPad = rightPad;
        this.indent = indent;
    }

    public void appendTitle(String title) {
        assert(title != null);
        appendHeader(title);
        appendSeparatorLine();
        labelSize = cells.size();
        labelWidth = width;
    }

    private void appendHeader(String text) {
        appendCell(new HeaderCell(text));
    }

    public void appendData(String text) {
        appendData(0, text, alignment);
    }

    public void appendData(String text, Align align) {
        appendData(0, text, align);
    }

    public void appendData(int level, String text) {
        appendData(level, text, alignment);
    }

    public void appendData(int level,
                           @SuppressWarnings("unused") String label,
                           String data) {
        appendData(level, data);
    }

    private void appendData(int level, String text, Align align) {
        appendCell(new Cell(text, align, level, indent));
    }

    public void appendEmptyData() {
        appendData("");
    }

    public void appendSeparatorLine() {
        appendCell(new SeparatorLineCell());
    }

    public void appendEmptyLine() {
        appendCell(new SeparatorLineCell(WHITESPACE));
    }

    private void appendCell(Cell cell) {
        cells.add(cell);
        if (cell.getAlign() != Align.UNALIGNED) {
            final int len = cell.getWidth();
            if (len > width) {
                width = len;
            }
        }
    }

    public int getHeight() {
        return cells.size();
    }

    public int getWidth() {
        return width + leftPad + rightPad;
    }

    public boolean isHeader(int index) {
        return cells.get(index).isHeader();
    }

    public boolean isSeparatorLine(int index) {
        return cells.get(index).isSeparatorLine();
    }

    public char getSeparatorLineChar(int index) {
        final Cell cell = cells.get(index);
        if (!cell.isSeparatorLine()) {
            throw new IllegalArgumentException("The specified cell is not "
                + "separator line");
        }
        return ((SeparatorLineCell)cell).getSeparatorChar();
    }

    public boolean isSeparatorEmptyLine(int index) {
        if (!isSeparatorLine(index)) {
            return false;
        }
        return (getSeparatorLineChar(index) == WHITESPACE);
    }

    public int getIndentLevel(int index) {
        final Cell cell = cells.get(index);
        return cell.getIndentLevel();
    }

    public void reset(boolean keepLabel, boolean keepWidth) {
        if (keepLabel) {
            final List<Cell> newList =
                new ArrayList<Cell>(cells.subList(0, labelSize));
            cells.clear();
            cells = newList;
            width = labelWidth;
        } else {
            cells.clear();
            if (!keepWidth) {
                width = 0;
            }
        }
    }

    public String getFormattedText(int index) {
        if (index < 0 || index >= cells.size()) {
            throw new IllegalArgumentException("Invalid cell: " + index);
        }

        final Cell cell = cells.get(index);
        String text = cell.getText();
        final int n = getWidth();
        switch (cell.getAlign()) {
            case LEFT:
                text = padLeft(text, text.length() + leftPad);
                text = padRight(text, n);
                break;
            case RIGHT:
                text = padRight(text, text.length() + rightPad);
                text = padLeft(text, n);
                break;
            case CENTER:
                text = centerAlign(text, n);
                break;
            default:
                break;
        }
        if (cell.isSeparatorLine()) {
            char separatorChar = ((SeparatorLineCell)cell).getSeparatorChar();
            if (separatorChar != WHITESPACE) {
                return text.replace(' ', separatorChar);
            }
        }
        return text;
    }

    public static String padLeft(String s, int n) {
        if (n == 0) {
            return s;
        }
        return String.format("%1$" + n + "s", s);
    }

    public static String padRight(String s, int n) {
        if (n == 0) {
            return s;
        }
        return String.format("%1$-" + n + "s", s);
    }

    public static String centerAlign(String s, int n) {
        if (n == 0) {
            return s;
        }
        int padding = n - s.length();
        if (padding <= 0) {
            return s;
        }
        int left = padding / 2 + s.length();
        return padRight(padLeft(s, left), n);
    }

    /**
     * The Cell class represents a element of column.
     */
    static class Cell {
        static final Align ALIGN_DEF = Align.LEFT;
        private final String text;
        private final Align align;
        private final int level;
        private final int indent;

        Cell(String text, Align align) {
            this(text, align, 0, 0);
        }

        Cell(String text, Align align, int level, int indent) {
            this.text = text;
            this.align = ((align == null)? ALIGN_DEF : align);
            this.level = level;
            this.indent = indent;
        }

        String getText() {
            if (level == 0 || indent == 0) {
                return text;
            }
            final int n = level * indent + text.length();
            return String.format("%1$" + n + "s", text);
        }

        Align getAlign() {
            return align;
        }

        int getWidth() {
            return indent * level + text.length();
        }

        int getIndentLevel() {
            return level;
        }

        boolean isData() {
            return true;
        }

        boolean isHeader() {
            return false;
        }

        boolean isSeparatorLine() {
            return false;
        }
    }

    /**
     * The header of table.
     */
    static class HeaderCell extends Cell {
        HeaderCell(String text) {
            super(text, Align.CENTER);
        }

        @Override
        boolean isHeader() {
            return true;
        }
    }

    /**
     * The cell that used as separator line.
     */
    static class SeparatorLineCell extends Cell {
        private char separatorChar;

        SeparatorLineCell() {
            this(SEPARATOR_DEF);
        }

        SeparatorLineCell(char ch) {
            super("", Align.LEFT);
            separatorChar = ch;
        }

        @Override
        boolean isSeparatorLine() {
            return true;
        }

        char getSeparatorChar() {
            return separatorChar;
        }
    }
}
