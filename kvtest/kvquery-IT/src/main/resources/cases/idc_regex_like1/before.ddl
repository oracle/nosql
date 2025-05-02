###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE playerinfo (
  id          INTEGER,
  id1         INTEGER,
  name        STRING,
  age         INTEGER,
  ballsbowled LONG,
  ballsplayed NUMBER,
  strikerate  DOUBLE,
  tier1rated  BOOLEAN,
  avg         FLOAT,
  fbin        BINARY(18),
  bin         BINARY,
  century     INTEGER not null default 100,
  country     STRING default "India",
  type        ENUM(international,domestic),
  time        timestamp(0),
  stats1      ARRAY(INTEGER),
  stats2      MAP(ARRAY(INTEGER)),
  stats3      RECORD(avg MAP(ARRAY(FLOAT))),
  stats4      ARRAY(RECORD(sr MAP(ARRAY(FLOAT)))),
  profile     STRING,
  info        JSON,
  primary key (shard(id),id1)
) using ttl 5 days


CREATE TABLE playerinfo.desc(
  id3     integer, 
  desc    string, 
  primary key(id3)
)

create index idx1 on playerinfo(country)
