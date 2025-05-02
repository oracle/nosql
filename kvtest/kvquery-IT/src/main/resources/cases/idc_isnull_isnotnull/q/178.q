#Expression returns empty with is not null in projection
select id,s.children."B.Balance" is not null from sn s where not exists s.children 