
create table foo(
  id integer,
  record record(int integer, double double, num number, str string, boo boolean),
  info json,
  primary key (id)
)

