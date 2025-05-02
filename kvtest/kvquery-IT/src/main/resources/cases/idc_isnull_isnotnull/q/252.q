#Expression in limit,offset clause using logical,exists operator(not empty),order by with parethesized expression and is not null in predicate and is null in projection
select id,age,long is null from sn s where age >10 and (long >5000 or exists double) is not null order by id  limit 3 offset 1
