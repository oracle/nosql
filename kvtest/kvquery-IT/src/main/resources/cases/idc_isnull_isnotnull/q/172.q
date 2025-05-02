#Expression returns non empty with is not null in predicate
select id,s.children."B.Balance" from sn s where exists s.children is not null
