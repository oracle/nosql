#Expression returns Json Atomic Types with is not null in projection
select s.children."B.Balance" is not null from sn s 