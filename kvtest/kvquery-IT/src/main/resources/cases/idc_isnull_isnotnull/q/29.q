#Expression returns timestamp with is null in predicate and enum is null in projection
select id,time is null from sn where type  is null