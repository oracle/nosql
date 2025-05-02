
create table users (
  uid integer,
  name string,
  email string,
  salary integer,
  primary key(uid))

create table users.folders (
  fid integer,
  name string,
  ancestors array(record(fid integer, name string)),
  children array(record(fid integer, name string)),
  primary key (fid)
)

create table users.folders.messages (
  mid integer,
  sender string,
  receiver string,
  time timestamp(3),
  size integer,
  content string,
  primary key(mid)
)

create table users.photos (
  pid integer,
  size integer,
  content binary,
  primary key(pid)
)


#
#               users
#              /     \
#             /       \
#          folders   photos
#           /
#          /
#       messages
#
