###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
#
# 1. arrays of complex values (there is a path after the multikey step)
#
# 1.1 target arrays nested (indirectly) inside arrays
#
# 1.1.1 target arrays do not contain nested arrays
#
# 1.1.2 target arrays contain nested arrays
#
# 2. arrays of atomic values (there is no path after the multikey step)
###
CREATE TABLE Foo(   
  id INTEGER,
  record RECORD(long LONG, int INTEGER, string STRING),
  info JSON,
  primary key (id)
)

create index idx_age_areacode_kind on foo (
    info.age as integer,
    info.addresses[].phones[].values().values().areacode as integer,
    info.addresses[].phones[].values().values().kind as anyAtomic)

create index idx_areacode_number_long on foo (
    info.addresses[].phones[].values().values().areacode as integer,
    info.addresses[].phones[].values().values().number as anyAtomic,
    record.long)

create index idx_keys_areacode_number on foo (
    info.addresses[].phones[].values().keys(),
    info.addresses[].phones[].values().values().areacode as integer,
    info.addresses[].phones[].values().values().number as anyAtomic)

create index idx_map1_keys_map2_values on foo (
    info.map1.keys(),
    info.map1.values().map2.values() as integer)
    

#create index idx_array on foo(info.maps.values().array[][][] as anyAtomic,
#                              info.age as integer)


