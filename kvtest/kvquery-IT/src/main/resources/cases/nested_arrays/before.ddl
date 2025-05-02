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
    info.addresses[].phones[][][].areacode as integer,
    info.addresses[].phones[][][].kind as anyAtomic)

create index idx_areacode_number_long on foo (
    info.addresses[].phones[][][].areacode as integer,
    info.addresses[].phones[][][].number as anyAtomic,
    record.long)

create index idx_state_areacode_kind on foo (
    info.addresses[].state as string,
    info.addresses[].phones[][][].areacode as integer,
    info.addresses[].phones[][][].kind as anyAtomic)

create index idx_state_areacode_city on foo (
    info.addresses[].state as string,
    info.addresses[].phones[][][].areacode as integer,
    info.addresses[].city as anyAtomic)

create index idx_array on foo(info.maps[].values().array[][][] as anyAtomic,
                              info.age as integer)

create index idx_keys_array on foo(
    info.maps[].keys(),
    info.maps[].values().array[][][] as anyAtomic,
    info.age as integer)

create index idx_array_foo_bar on foo(
    info.maps[].values().array[][][] as anyAtomic,
    info.maps[].keys(),
    info.maps[].values().foo as anyAtomic,
    info.maps[].values().bar as anyAtomic)

create index idx_foo_bar on foo(
    info.maps[].values().foo as anyAtomic,
    info.maps[].values().bar as anyAtomic)



CREATE TABLE Bar(   
  id INTEGER,
  record RECORD(long LONG, int INTEGER, string STRING),
  info JSON,
  primary key (id)
)


create index idx_areacode_number_long on bar (
    info.addresses[].phones[][][].areacode as integer,
    info.addresses[].phones[][][].number as anyAtomic,
    record.long)
    with unique keys per row


create index idx_state_areacode_kind on bar (
    info.addresses[].state as string,
    info.addresses[].phones[][][].areacode as integer,
    info.addresses[].phones[][][].kind as anyAtomic)
    with unique keys per row


create table netflix(
   acct_id integer,
   user_id integer,
   value json,
   primary key(acct_id, user_id))


create index idx_showid on netflix(
    value.contentStreamed[].showId as integer)
    with unique keys per row

create index idx_country_showid on netflix(
    value.country as string,
    value.contentStreamed[].showId as integer)
    with unique keys per row

create index idx_showid_minWatched on netflix(
    value.contentStreamed[].showId as integer,
    value.contentStreamed[].seriesInfo[].episodes[].minWatched as integer)
    with unique keys per row

create index idx_showid_seasonNum on netflix(
    value.contentStreamed[].showId as integer,
    value.contentStreamed[].seriesInfo[].seasonNum as integer)
    with unique keys per row

create index idx_showid_seasonNum_minWatched on netflix(
    value.contentStreamed[].showId as integer,
    value.contentStreamed[].seriesInfo[].seasonNum as integer,
    value.contentStreamed[].seriesInfo[].episodes[].minWatched as integer)
    with unique keys per row
