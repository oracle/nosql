#Expression returns empty with is null in predicate
select id,s.children."B.Balance" from sn s where not exists s.children is null