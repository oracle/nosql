#TestDescription: SET TTL with new TTL value
#Expected result: ttl should be set to new value

insert into playerinformation $row1 (id,id1,name,age) values (
100,
208,
"ttl_expr",
41
)
set ttl 7 days
returning $row1 as row, remaining_days($row1) as ttl

