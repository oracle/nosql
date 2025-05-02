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

create index idx_state_city_age on foo (
    info.address.state as string,
    info.address.city as string,
    info.age as integer)

create index idx_state_areacode_age on foo (
    info.address.state as string,
    info.address.phones[].areacode as integer,
    info.age as integer)

create index idx_areacode_kind on foo (
    info.address.phones[].areacode as integer,
    info.address.phones[].kind as string)


create index idx_children_anna_friends on foo (
    info.children.Anna.friends[] as string)

create index idx_kids_anna_friends on foo (
    info.children.anna.friends[] as string)

create index idx_children_both on foo (
    info.children.keys(),
    info.children.values().age as long,
    info.children.values().school as string)

create index idx_children_values on foo (
    info.children.values().age as long,
    info.children.values().school as string)

create index idx_anna_areacode on foo (
    info.children.Anna.age as long,
    info.address.phones[].areacode as integer)

create index idx1 on Foo (info.children."[Dave".keys())

create index idx_children_keys on foo (
    info.address.city as string,
    info.children.keys())


CREATE TABLE Bar(   
  id INTEGER,
  record RECORD(long LONG, int INTEGER, string STRING, bool BOOLEAN, float FLOAT),
  info JSON,
  primary key (id)
)

create index idx_state_city_age on bar (
    info.address.state as anyAtomic,
    info.address.city as anyAtomic,
    info.age as integer)

create index idx_product_agreementIdTag on bar(
    info.agreement[].agreementId as STRING,
    info.tag as STRING)

create index idx_product_tag on bar (
    info.tag AS string)
