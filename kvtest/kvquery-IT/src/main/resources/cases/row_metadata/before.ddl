
CREATE TABLE Foo(   
  id INTEGER,
  info JSON,
  primary key (id)
)

create index idx_state_city_age on foo (
    row_metadata().address.state as string,
    row_metadata().address.city as string,
    row_metadata().age as integer)

create index idx_state_areacode_age on foo (
    row_metadata().address.state as string,
    row_metadata().address.phones[].areacode as integer,
    row_metadata().age as integer)

create index idx_areacode_kind on foo (
    row_metadata().address.phones[].areacode as integer,
    row_metadata().address.phones[].kind as string)
with unique keys per row

create index idx_areacode_kind_2 on foo (
    info.address.phones[].areacode as integer,
    info.address.phones[].kind as string)
with unique keys per row


create index idx_children_anna_friends on foo (
     row_metadata().children.Anna.friends[] as string)

create index idx_kids_anna_friends on foo (
    row_metadata().children.anna.friends[] as string)

create index idx_children_both on foo (
    row_metadata().children.keys(),
    row_metadata().children.values().age as long,
    row_metadata().children.values().school as string)

create index idx_children_values on foo (
    row_metadata().children.values().age as long,
    row_metadata().children.values().school as string)

create index idx_anna_areacode on foo (
    row_metadata().children.Anna.age as long,
    row_metadata().address.phones[].areacode as integer)

create index idx_children_keys on foo (
    row_metadata().address.city as string,
    row_metadata().children.keys())


CREATE TABLE Bar(id INTEGER, primary key (id)) as json collection

create index idx_state_city_age on bar (
    row_metadata().address.state as string,
    row_metadata().address.city as string,
    row_metadata().age as integer)

create index idx_state_areacode_age on bar (
    row_metadata().address.state as string,
    row_metadata().address.phones[].areacode as integer,
    row_metadata().age as integer)

create index idx_areacode_kind on bar (
    row_metadata().address.phones[].areacode as integer,
    row_metadata().address.phones[].kind as string)
with unique keys per row

create index idx_areacode_kind_2 on bar (
    info.address.phones[].areacode as integer,
    info.address.phones[].kind as string)
with unique keys per row


create index idx_children_anna_friends on bar (
     row_metadata().children.Anna.friends[] as string)

create index idx_kids_anna_friends on bar (
    row_metadata().children.anna.friends[] as string)

create index idx_children_both on bar (
    row_metadata().children.keys(),
    row_metadata().children.values().age as long,
    row_metadata().children.values().school as string)

create index idx_children_values on bar (
    row_metadata().children.values().age as long,
    row_metadata().children.values().school as string)

create index idx_anna_areacode on bar (
    row_metadata().children.Anna.age as long,
    row_metadata().address.phones[].areacode as integer)

create index idx_children_keys on bar (
    row_metadata().address.city as string,
    row_metadata().children.keys())


