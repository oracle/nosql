#Expression in limit,offset clause using logical,exists operator(not empty),order by with parethesized expression and is not null in predicate and projection
select id,age,long is not null from sn s where age >10 and (long >5000 or exists double) is not null order by id  limit 20 offset 5
