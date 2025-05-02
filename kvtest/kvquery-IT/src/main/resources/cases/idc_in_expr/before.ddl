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

create index if not exists idx_barNew1234 on fooNew (info.bar1 as integer,
                                 info.bar2 as double,
                                 info.bar3 as string,
                                 info.bar4 as long)

create index if not exists idx_fooNewphones on fooNew (info.phones[].num as integer,
                                info.phones[].kind as string)


create table if not exists SimpleDatatype(
    id integer, 
    name string, 
    salary double, 
    rank long, 
    rating double, 
    isEmp boolean, 
    enm ENUM(tok1,tok2,tok3), 
    lastname string, 
    primary key (id)
)

create table if not exists ComplexType(
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER,
  ptr STRING,
  address RECORD( city STRING,
                  state STRING,
                  phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                  ptr STRING,
         	  test STRING),
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  lng long , 
  flt float, 
  dbl double, 
  bool boolean,
  enm  ENUM(tok1, tok2, tok3, tok4),
  bin  BINARY,
  fbin  BINARY(10),
  primary key (id)
)

create index if not exists idx_flt on ComplexType (flt)

