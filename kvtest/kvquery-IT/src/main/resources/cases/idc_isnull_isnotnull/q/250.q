#Expression in limit,offset clause using logical,sequecnce operator,exists operator(not empty),order by with parethesized expression and is not null in predicate
select id,age,long from sn s where age >10 and (long >any 5000 or exists double) is not null order by id  limit 3 offset 1 
