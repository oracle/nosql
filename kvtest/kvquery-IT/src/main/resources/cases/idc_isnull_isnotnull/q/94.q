#Expression returns Json Atomic Int with is not null in predicate
select s.children.Matt.age from sn s where s.children.Matt.age is not null