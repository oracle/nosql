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

package oracle.kv.impl.admin;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import oracle.kv.impl.api.table.Region;
import oracle.kv.impl.api.table.RegionMapper;

import oracle.kv.impl.security.KVStorePrivilege;
import oracle.kv.impl.security.OperationContext;
import oracle.kv.impl.security.SystemPrivilege;


/**
 * Region DDL operations.
 */
public abstract class RegionDdlOperation implements DdlHandler.DdlOperation {

    private final String opName;

    /* May be null */
    private final String regionName;

    protected RegionDdlOperation(String opName, String regionName) {
        this.opName = opName;
        this.regionName = regionName;
    }

    protected String opName() {
        return opName;
    }

    protected String getRegionName() {
        return regionName;
    }

    @Override
    public OperationContext getOperationCtx() {
        return new OperationContext() {
            @Override
            public String describe() {
                return regionName == null ? opName : opName + " " + regionName;
            }

            @Override
            public List<? extends KVStorePrivilege> getRequiredPrivileges() {
                return getPriv();
            }
        };
    }

    protected abstract List<SystemPrivilege> getPriv();

    public static class CreateRegion extends RegionDdlOperation {

        public CreateRegion(String regionName) {
            super("CREATE REGION", regionName);
        }

        @Override
        protected List<SystemPrivilege> getPriv() {
            return SystemPrivilege.regionCreatePrivList;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            String exceptionMsg;
            try {
                final Admin admin = ddlHandler.getAdmin();
                final int planId =
                       admin.getPlanner().createAddRegionPlan("CreateRegion",
                                                              getRegionName());
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            ddlHandler.operationFails(opName() + " failed: " + exceptionMsg);
        }
    }

    public static class DropRegion extends RegionDdlOperation {

        public DropRegion(String regionName) {
            super("DROP REGION", regionName);
        }

        @Override
        protected List<SystemPrivilege> getPriv() {
            return SystemPrivilege.regionDropPrivList;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            String exceptionMsg;
            try {
                final Admin admin = ddlHandler.getAdmin();
                final int planId =
                       admin.getPlanner().createDropRegionPlan("DropRegion",
                                                               getRegionName());
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            ddlHandler.operationFails(opName() + " failed: " + exceptionMsg);
        }
    }

    static class SetLocalRegionName extends RegionDdlOperation {

        public SetLocalRegionName(String regionName) {
            super("SET LOCAL REGION", regionName);
        }

        @Override
        protected List<SystemPrivilege> getPriv() {
            return SystemPrivilege.regionSetLocalPrivList;
        }

        @Override
        public void perform(DdlHandler ddlHandler) {
            String exceptionMsg;
            try {
                final Admin admin = ddlHandler.getAdmin();
                final int planId = admin.getPlanner().
                                createSetLocalRegionNamePlan("SetLocalRegionName",
                                                             getRegionName());
                ddlHandler.approveAndExecute(planId);
                return;
            } catch (IllegalCommandException ice) {
                exceptionMsg = ice.getMessage();
            }
            ddlHandler.operationFails(opName() + " failed: " + exceptionMsg);
        }
    }

    public static class ShowRegions extends RegionDdlOperation {
        private final boolean asJson;

        public ShowRegions(boolean asJson) {
            super(asJson ? "SHOW REGIONS" : "SHOW AS JSON REGIONS", null);
            this.asJson = asJson;
        }

        @Override
        protected List<SystemPrivilege> getPriv() {
            return SystemPrivilege.dbviewPrivList;
        }

        /**
         * Generates a list of region names in the following order (if present):
         *  - local region
         *  - active remote region(s)
         *  - dropped remote region(s)
         */
        @Override
        public void perform(DdlHandler ddlHandler) {
            final RegionMapper rm = ddlHandler.getTableMetadata().
                                                            getRegionMapper();
            String localName = null;
            final SortedSet<String> active = new TreeSet<>();
            final SortedSet<String> dropped = new TreeSet<>();

            if (!rm.isEmpty()) {
                for (String name : rm.getKnownRegions().values()) {
                    final int id = rm.getRegionId(name);
                    switch (id) {
                    case Region.LOCAL_REGION_ID:
                        localName = name;
                        break;
                    case Region.UNKNOWN_REGION_ID:
                        dropped.add(name);
                        break;
                    default:
                        active.add(name);
                        break;
                    }
                }
            }
            final StringBuilder sb = new StringBuilder();
            if (asJson) {
                boolean first = true;
                sb.append("{\"regions\" : [");
                if (localName != null) {
                    formatRegion(sb, first, localName, "local", "active");
                    first = false;
                }
                for (String name : active) {
                    formatRegion(sb, first, name, "remote", "active");
                    first = false;
                }
                for (String name : dropped) {
                    formatRegion(sb, first, name, "remote", "dropped");
                    first = false;
                }
                sb.append("]}");
            } else {
                sb.append("regions");
                if (localName != null) {
                    sb.append("\n  ");
                    sb.append(localName).append(" (local, active)");
                }
                for (String name : active) {
                    sb.append("\n  ");
                    sb.append(name).append(" (remote, active)");
                }
                for (String name : dropped) {
                    sb.append("\n  ");
                    sb.append(name).append(" (remote, dropped)");
                }
            }
            ddlHandler.operationSucceeds();
            ddlHandler.setResultString(sb.toString());
        }
    }

    private static void formatRegion(StringBuilder sb,
                                     boolean first,
                                     String name,
                                     String type,
                                     String state) {
        if (!first) {
            sb.append(",");
        }
        sb.append("{\"name\" : \"");
        sb.append(name);
        sb.append("\", \"type\" : \"");
        sb.append(type);
        sb.append("\", \"state\" : \"");
        sb.append(state);
        sb.append("\"}");
    }
}
