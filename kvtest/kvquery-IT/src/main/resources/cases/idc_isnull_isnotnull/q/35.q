#Expression returns timetamp with is not null in projection and also is not null in predicate
select id,time is not null from sn where time is not null