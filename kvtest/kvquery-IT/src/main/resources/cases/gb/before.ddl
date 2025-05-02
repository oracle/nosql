###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo
(   
  id1 INTEGER,
  id2 INTEGER,
  id3 INTEGER,
  record RECORD(long LONG, 
                int INTEGER,
                double DOUBLE,
                str STRING,
                bool BOOLEAN
               ),
  xact JSON,
  mixed JSON,
  primary key (shard(id1), id2, id3)
)


create index idx_long_bool on Foo (record.long, record.bool)


create index idx_acc_year_prodcat on Foo (xact.acctno as integer,
                                          xact.year as integer,
                                          xact.prodcat as string)

create index idx_store on Foo (xact.storeid as anyatomic)


CREATE TABLE Bar
(   
  id1 INTEGER,
  id2 INTEGER,
  id3 INTEGER,
  xact JSON,
  primary key (shard(id1), id2, id3)
)


create index idx_acc_year_prodcat on Bar (xact.acctno as integer,
                                          xact.year as integer,
                                          xact.prodcat as string)

create index idx_year_price on Bar (xact.year as integer,
                                    xact.items[].qty as integer)

create index idx_state on Bar (xact.state as string)


create table empty
(
  id1 INTEGER,
  id2 INTEGER,
  id3 INTEGER,
  record RECORD(long LONG, 
                int INTEGER,
                double DOUBLE,
                str STRING,
                bool BOOLEAN
               ),
  xact JSON,
  primary key (shard(id1), id2, id3)
)

create table tsminmax
(
  id INTEGER,  
  ts3 TIMESTAMP(3), 
  mts6 MAP(TIMESTAMP(6)),
  primary key(id)
)


create table numbers(id integer, number json, decimal number, primary key (id))


CREATE TABLE Boo
(   
  id1 INTEGER,
  acctno integer,
  xact JSON,
  primary key (id1)
)


create index idx_acc_year on Boo (acctno, xact.year as integer)



create table if not exists fooNew(
  id integer,
  foo1 integer,
  foo2 double,
  foo3 string,
  foo4 long,
  info json,
  primary key (id)
)

create index if not exists idx_fooNew1234 on fooNew (foo1, foo2, foo3, foo4)

create index if not exists idx_barNew1234 on fooNew (
  info.bar1 as integer,
  info.bar2 as double,
  info.bar3 as string,
  info.bar4 as long)

create index if not exists idx_fooNewphones on fooNew (
  info.phones[].num as integer,
  info.phones[].kind as string)


create table XYZ(id integer, j json, s string, primary key(id))
