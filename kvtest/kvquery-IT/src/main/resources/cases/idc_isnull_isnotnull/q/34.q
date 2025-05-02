#Expression returns enum with is not null in projection and is null in predicate
select id,time is not null from sn where time is null