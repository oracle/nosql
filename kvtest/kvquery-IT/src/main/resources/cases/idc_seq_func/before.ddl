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
  stats1      MAP( RECORD(matches integer, inns integer, notout integer, runs long, hs integer, avg double, bf number, sr double, century integer,fifty integer, fours integer,sixes integer)),
  stats2      ARRAY(MAP(ARRAY(INTEGER))),
  stats3      RECORD(city STRING,
                     country STRING,
                     runs ARRAY( RECORD ( test INTEGER, odi INTEGER, t20 INTEGER ) ),
                     ptr STRING),
  json        JSON,
  primary key (shard(id),id1)
) using ttl 5 days


create table stats(id integer, statsinfo json, primary key(id))

create index idx_matches_runs on stats(statsinfo.runs as integer,
                                       statsinfo.matches as integer)

create table testarray(id integer, arrstr array(string), arrbool array(boolean), info json, primary key(id))
