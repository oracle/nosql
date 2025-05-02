#Expression returns empty with is not null in predicate
select id,s.children."B.Balance" from sn s where not exists s.children is not null