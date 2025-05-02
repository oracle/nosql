#Expression returns Json Atomic Types with is not null in predicate
select s.children."B.Balance" from sn s where  s.children.B.Balance is not null