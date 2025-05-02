
CREATE TABLE Viewers(
  acct_id integer,
  user_id integer,
  primary key(shard(acct_id), user_id)) as json collection


create index idx_country_genre on Viewers(
    country as string,
    shows[].genres[] as string)

create index idx_country_showid_date on Viewers (
    country as string,
    shows[].showId as integer,
    shows[].seasons[].episodes[].date as string)

create index idx_showid on Viewers (
    shows[].showId as integer)
with unique keys per row

create index idx_country_showid on Viewers (
    country as string,
    shows[].showId as integer)
with unique keys per row

create index idx_country_showid_seasonnum_minWatched on Viewers (
    country as string,
    shows[].showId as integer,
    shows[].seasons[].seasonNum as integer,
    shows[].seasons[].episodes[].minWatched as integer,
    shows[].seasons[].episodes[].episodeID as integer)
with unique keys per row


create table if not exists jsoncol(
  majorKey1 string,
  majorKey2 string,
  minorKey string,
  primary key(shard(majorKey1,majorKey2),minorKey)) as json collection

create index idx_index on jsoncol(index as long)

create index idx_name on jsoncol(address.name as string)
