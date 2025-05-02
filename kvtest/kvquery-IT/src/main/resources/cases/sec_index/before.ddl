###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(   
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER,
  ptr STRING,       
  address RECORD(   
          city STRING,
          state STRING,
          phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
          ptr STRING), 
  children MAP( RECORD( age LONG, iage INTEGER, friends ARRAY(STRING) ) ),
  primary key (id)
) using ttl 5 days


CREATE INDEX idx_state_city_age ON Foo (address.state, address.city, age)

CREATE INDEX idx_children_anna on Foo (children.Anna.iage)

CREATE TABLE Boo(   
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER default 25,
  primary key (firstName, lastName)
)

CREATE INDEX idx_age on Boo (age)


create table T1(id integer, name string, primary key(id))

create index idx1 on T1(name)

create table T2(id integer, info json, primary key(id))

create index idx1 on T2(info.name as string)

create table keyOnly(
  firstName string,
  lastName string, 
  age integer, 
  id integer, 
  primary key(shard(lastName, firstName), age, id)
)

create index First on keyOnly (firstName)


CREATE TABLE Bar(
  fld_sid INTEGER,
  fld_id INTEGER,
  fld_str STRING,
  PRIMARY KEY(SHARD(fld_sid), fld_id))

create index idx_str on Bar (fld_str)
