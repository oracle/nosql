###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE User (
     id INTEGER,
     firstName STRING,
     lastName STRING,
     age INTEGER,
     addresses ARRAY(RECORD(
             city STRING,
             state STRING,
             phones ARRAY(ARRAY(RECORD(
                  areacode INTEGER,
                  number INTEGER,
                  kind STRING)))
             )),
     children MAP(RECORD( age LONG, school STRING, friends ARRAY(STRING))),
     primary key (id)
)

CREATE INDEX idx_state_city_age ON User (addresses[].state,
                                         addresses[].city,
                                         age) with unique keys per row

CREATE INDEX idx_state_areacode_kind  ON User (addresses[].state,
                                               addresses[].phones[][].areacode,
                                               addresses[].phones[][].kind) with unique keys per row

CREATE INDEX idx_children_both ON User (children.keys(),
                                        children.values().age,
                                        children.values().school) with unique keys per row

CREATE INDEX idx_areacode_kind ON User (addresses[].phones[][].areacode,
                                        addresses[].phones[][].kind) with unique keys per row


