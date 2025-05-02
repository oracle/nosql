#Expression returns empty with is null in projection
select id,s.children."B.Balance" is null from sn s where not exists s.children