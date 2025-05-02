#Expression returns Json Atomic Types with is null in projection
select s.children."B.Balance" is null from sn s 