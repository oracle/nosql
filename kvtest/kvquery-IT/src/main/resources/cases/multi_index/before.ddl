###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(
   id1 INTEGER,
   id2 INTEGER,
   id3 LONG,
   rec RECORD(a INTEGER,
              b ARRAY(INTEGER),
              c ARRAY(RECORD(ca INTEGER)),
              d ARRAY(MAP(INTEGER)),
              f FLOAT),
              primary key(shard(id1, id2), id3))

CREATE INDEX idx_a_c_f ON Foo (rec.a, rec.c[].ca, rec.f)

CREATE INDEX idx_d_f ON Foo (rec.d[].d2, rec.f, rec.d[].d3)

CREATE INDEX idx_a_id2 ON Foo (rec.a, id2)

#
# call the index "idx_a_rf" instead of "idx_a_f" so that it will be processed
# after idx_a_id2
#
CREATE INDEX idx_a_rf ON Foo (rec.a, rec.f)

CREATE INDEX idx_id3 ON Foo (id3)


create table usage (
    tenantId String,
    tableName string,
    startSeconds integer,
    primary key(shard(tenantId, tableName), startSeconds)
) using ttl 14 days


create table bar(pk string, primary key(pk))



create table if not exists historical0 (
    deviceId string,
    type enum(THERMOMETER, HVAC),
    timestamp long,
    data json,
primary key (deviceId, timestamp))


create index idx_type on historical0 (type)

create index idx_temp on historical0 (type, data.temperature as double)

create index idx_hvac on historical0 (type, data.mode as string)
