###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(   
  id INTEGER,
  record RECORD(long LONG, int INTEGER, string STRING, bool BOOLEAN, float FLOAT),
  info JSON,
  primary key (id)
)

CREATE INDEX idx_state_city_age ON Foo (
    info.address.state as string,
    info.address.city as string)

create index idx_state_areacode_kind on foo (
    info.address.state as string,
    info.address.phones[].areacode as integer,
    info.address.phones[].kind as string) with unique keys per row

create index idx_areacode_kind on foo (
    info.address.phones[].areacode as integer,
    info.address.phones[].kind as string) with unique keys per row

CREATE INDEX idx_children_both ON Foo (
    info.children.keys(),
    info.children.values().age as integer,
    info.children.values().school as string) with unique keys per row

CREATE INDEX idx_children_values ON Foo (
    info.children.values().age as integer,
    info.children.values().school as string) with unique keys per row


