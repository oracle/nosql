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

package oracle.kv.impl.systables;

import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.table.FieldDef;


/**
 * Descriptor for system table information.
 *
 * A descriptor defines static fields for the table name and the table's
 * fields. These constants are used by various components that access the
 * table.
 *
 * An instance of the descriptor is used by the Admin to create and
 * upgrade system tables in a generic manner. For this purpose here should
 * be one instance of SysTableDescriptor in the SysTableRegistry for each
 * system table.
 */
public abstract class SysTableDescriptor {

    private static final IndexDescriptor[] EMPTY = new IndexDescriptor[0];

    /**
     * Creates a system table name from the specified name string.
     */
    protected static String makeSystemTableName(String name) {
        return TableImpl.SYSTEM_TABLE_PREFIX + name;
    }

    /**
     * Returns the table name.
     *
     * @return the table name
     */
    public abstract String getTableName();

    /**
     * Returns the schema version of this descriptor.
     */
    protected abstract int getCurrentSchemaVersion();

    /**
     * Return whether snapshot load should restore contents of this table.
     */
    public abstract boolean isRestore();

    /**
     * Returns a table built from this description.
     *
     * @return a table
     */
    public final TableImpl buildTable() {
        final TableBuilder builder =
            TableBuilder.createSystemTableBuilder(getTableName());
        builder.setDescription(
                getSchemaVersionString(getCurrentSchemaVersion()));
        buildTable(builder);
        return builder.buildTable();
    }

    /**
     * Descriptor specific build method. The table name and description
     * (schema version) is already set in the builder.
     */
    protected abstract void buildTable(TableBuilder builder);

    /**
     * Evolves the specified table if necessary. If the input table needed to
     * be evolved the evolved table is returned otherwise null is returned.
     *
     * @param table input table
     *
     * @return the evolved table or null
     */
    public final TableImpl evolveTable(TableImpl table, Logger logger) {
        assert table.isSystemTable();

        final int schemaVersion = getTableSchemaVersion(table);
        if (schemaVersion >= getCurrentSchemaVersion()) {
            return null;
        }

        try {
            final TableEvolver ev = TableEvolver.createTableEvolver(table);
            final int newVersion = evolveTable(ev, schemaVersion);

            assert newVersion == getCurrentSchemaVersion();

            ev.setDescription(getSchemaVersionString(newVersion));
            return ev.evolveTable();
        } catch (Exception ex) {
            logger.log(Level.WARNING,
                       "Unexpected exception upgrading system table {0}" +
                       " from version {1} to {2} : {3}",
                       new Object[]{table.getFullName(),
                                    schemaVersion, getCurrentSchemaVersion(),
                                    ex.getMessage()});
        }
        return null;
    }

    /**
     * Evolve a table from the specified schema version. Returns the new
     * version of the table. A subclass should override this method to
     * upgrade the table to the current version.
     *
     * The default implementation throws IllegalStateException to catch
     * coding errors.
     */
    protected int evolveTable(@SuppressWarnings("unused") TableEvolver ev,
                              int schemaVersion) {
        throw new IllegalStateException("Attempt to evolve system table" +
                                        " described by " + this +
                                        " at version " + schemaVersion +
                                        " to " + getCurrentSchemaVersion());
    }

    /**
     * Converts a schema version to a string to be used in the table's
     * description field.
     */
    private String getSchemaVersionString(int schemaVersion) {
        return Integer.toString(schemaVersion);
    }

    /**
     * Returns the schema version found in the table's description.
     */
    protected int getTableSchemaVersion(TableImpl table) {
        /* A null description indicates pre schema version, so assign to 1 */
        final String description = table.getDescription();
        return description == null ? 1 : Integer.parseInt(description);
    }

    /**
     * Returns an index descriptor for an index which is not already present on
     * the specified table. If no new indexes are needed null is returned.
     */
    public IndexDescriptor getIndexDescriptor(TableImpl table) {
        assert table.isSystemTable();

        for (IndexDescriptor desc : getIndexDescriptors()) {
            if (table.getIndex(desc.getIndexName()) == null) {
                return desc;
            }
        }
        return null;
    }

    /**
     * Returns an array of index descriptors for all of the indexes specified
     * in the system table descriptor for this table. Returns an empty array if
     * the table has no indexes specified.
     *
     * Public for unit tests.
     */
    public IndexDescriptor[] getIndexDescriptors() {
        return EMPTY;
    }

    @Override
    public String toString() {
        return "SysTableDescriptor[" + getTableName() + ", " +
               getCurrentSchemaVersion() + "]";
    }

    /**
     * Descriptor for optional index on a system table.
     */
    public interface IndexDescriptor {

        /**
         * Returns the index name.
         */
        String getIndexName();

        /**
         * Returns the index field(s).
         */
        String[] getIndexedFields();

        /**
         * Returns the index field type(s), or null. A non-null value with
         * non-null entries is only needed for index fields of type JSON. The
         * default implementation returns null.
         */
        default FieldDef.Type[] getIndexedTypes() { return null;}

        /**
         * Returns index description.
         */
        String getDescription();
    }
}
