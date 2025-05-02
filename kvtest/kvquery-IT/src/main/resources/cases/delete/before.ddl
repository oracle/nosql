
CREATE TABLE Foo
(   
  id INTEGER,
  info JSON,
  primary key (id)
)

create index idx_name on foo (info.lastName as string,
                              info.firstName as string)


create index idx_areacode on foo(info.address.phones[].areacode as integer)
