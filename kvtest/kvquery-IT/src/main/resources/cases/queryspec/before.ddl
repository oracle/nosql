CREATE TABLE Users (
  id INTEGER,
  firstName STRING,
  lastName STRING,
  otherNames ARRAY(RECORD(first STRING, last STRING)),
  age INTEGER,
  income INTEGER,
  address JSON,
  expenses MAP(INTEGER),
  connections ARRAY(INTEGER),
  PRIMARY KEY (id)
) using ttl 3 hours

create index idx_income on Users (income)

create index idx_state_city_income on Users (
  address.state as string,
  address.city as string,
  income)

create index idx_expenses_books on Users (expenses.books)

create index idx_connections on Users(connections[])

create index idx_areacodes_income on Users (
    address.phones[].area as anyAtomic,
    income)

create index idx_areacodes_kinds_income on Users (
    address.phones[].area as anyAtomic,
    address.phones[].kind as string,
    income)

create index idx_expenses_keys_values on Users (
    expenses.keys(),
    expenses.values())

create index idx_year_month on Users(
    substring(address.startDate as string, 0, 4),
    substring(address.startDate as string, 5, 2)
)

create index idx_substring on Users(substring(otherNames[].first, 1,2),
                                    length(otherNames[].last)) 


create index idx_exp_time on Users (expiration_time())

create index idx_mod_time on Users (modification_time())

create index idx_row_size on Users (row_storage_size())

CREATE TABLE Viewers (
  acct_id integer,
  user_id integer, 
  info json,
  primary key(acct_id, user_id)
)


create index idx1_country_genre on Viewers(
    info.country as string,
    info.shows[].genres[] as string)

create index idx2_country_showid_date on Viewers (
    info.country as string,
    info.shows[].showId as integer,
    info.shows[].seasons[].episodes[].date as string)


create index idx3_country on Viewers(
    info.country as string)

create index idx4_showid on Viewers (
    info.shows[].showId as integer)
with unique keys per row

create index idx5_country_showid on Viewers (
    info.country as string,
    info.shows[].showId as integer)
with unique keys per row

create index idx6_country_showid_seasonnum_minWatched on Viewers (
    info.country as string,
    info.shows[].showId as integer,
    info.shows[].seasons[].seasonNum as integer,
    info.shows[].seasons[].episodes[].minWatched as integer,
    info.shows[].seasons[].episodes[].episodeID as integer)
with unique keys per row

create index idx7_country_showid_date on Viewers (
    info.country as string,
    info.shows[].showId as integer,
    info.shows[].seasons[].episodes[].date as string,
    info.shows[].seasons[].episodes[].episodeID as integer)
with unique keys per row

create index idx8_showid_year_month on Viewers (
    info.shows[].showId as integer,
    substring(info.shows[].seasons[].episodes[].date as string, 0, 4),
    substring(info.shows[].seasons[].episodes[].date as string, 5, 2))

create index idx9_year_month on Viewers (
    substring(info.shows[].seasons[].episodes[].date as string, 0, 4),
    substring(info.shows[].seasons[].episodes[].date as string, 5, 2))


CREATE TABLE Users2 (
  id INTEGER,
  income INTEGER,
  address RECORD(street STRING,
                 city STRING,
                 state STRING,
                 phones ARRAY(RECORD(area INTEGER,
                                     number INTEGER,
                                     kind STRING))
                ),
  matrixes ARRAY(RECORD(mid INTEGER,
                        mdate TIMESTAMP(0),
                        matrix ARRAY(MAP(ARRAY(RECORD(foo INTEGER, bar INTEGER))))
                       )
                ),
  connections ARRAY(INTEGER),
  expenses MAP(INTEGER),
  PRIMARY KEY (id)
)



create index idx1 on Users2 (income)

create index idx2 on Users2 (address.state, address.city, income)

create index idx3 on Users2 (expenses.books)

create index idx4 on users2 (expenses.housing, expenses.travel)

create index midx1 on Users2 (connections[])

create index midx2 on Users2 (address.phones[].area, income)

create index midx3 on Users2 (address.state, expenses.values())

create index midx4 on Users2 (matrixes[].matrix[].values()[].foo)

create index midx5 on Users2 (address.phones[].area, address.phones[].kind, income)

create index midx6 on Users2 (expenses.keys(), expenses.values())

create index midx7 on Users2(
  matrixes[].mdate,
  matrixes[].matrix[].keys(),
  matrixes[].matrix[].values()[].bar)

create index midx8 on Users2(
  matrixes[].mdate,
  matrixes[].matrix[].key1[].bar)

# midx3 --> midx5

