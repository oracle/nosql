
create table profile(uid long,
                     userName string,
                     emailAddr string,
                     age integer,
primary key(uid))

create table profile.inbox(msgid long, content json, primary key(msgid))

create table profile.sent(msgid long, primary key(msgid))

create table profile.deleted(msgid long, primary key(msgid))

create table profile.messages(msgid long, content JSON, primary key(msgid))


create index idx1_msgs_sender on profile.messages(content.sender as string)

create index idx2_msgs_receivers on profile.messages(content.receivers[] as string)
with unique keys per row

create index idx3_msgs_size on profile.messages(content.size as integer)

create index idx5_msgs_views on profile.messages(content.views[] as string)
with unique keys per row


create table A(sid string, id integer, s string, primary key(shard(sid), id))

create table A.B(bid integer, s string, primary key(bid))

create index idxS on A(s)

create index idxS on A.B(s)

