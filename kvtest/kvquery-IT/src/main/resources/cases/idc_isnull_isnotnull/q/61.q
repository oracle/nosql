#Expression returns Json Atomic types with is null in predicate
select s.children."B.Balance" from sn s where s.children.B.Balance is null