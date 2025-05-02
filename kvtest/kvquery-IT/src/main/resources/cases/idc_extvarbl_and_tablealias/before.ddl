###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE users(
  id integer,
  firstname string,
  lastname string,
  age integer,
  income integer,
  married boolean,
  address RECORD(city STRING,
                 state STRING,
                 phones ARRAY( RECORD ( work INTEGER, home INTEGER) ), 
                 ptr STRING),
  children MAP(RECORD( age LONG, friends ARRAY(STRING) ) ),
  primary key (id)
)

CREATE TABLE select(
  id integer,
  select String,
  from String,	
  primary key (id)
)

CREATE TABLE from(
  id integer,
  from String,
  select String,
  primary key (id)
)

