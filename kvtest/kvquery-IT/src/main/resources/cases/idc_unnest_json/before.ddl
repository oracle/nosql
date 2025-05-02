##
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE User_json (
     id INTEGER,
     info  JSON,
     primary key(id)
)

CREATE INDEX idx_state_city_age ON User_json (info.addresses[].state as STRING, 
                               info.addresses[].city as STRING, 
                               info.age as INTEGER) with unique keys per row

CREATE INDEX idx_phones ON User_json (info.addresses[].state as STRING,
                                info.addresses[].phones[][].areacode as INTEGER,
                                info.addresses[].phones[][].kind as STRING)  with unique keys per row

CREATE INDEX idx_phones2 ON User_json (info.addresses[].phones[][].areacode as INTEGER,
                                info.addresses[].phones[][].kind as STRING) with unique keys per row

CREATE INDEX idx_children_both ON User_json (info.children.keys(),
                                info.children.values().age as INTEGER,
                                info.children.values().school as STRING) with unique keys per row

CREATE INDEX idx_children_values ON User_json (
                                info.children.values().age as INTEGER,
                                info.children.values().school as STRING) with unique keys per row

