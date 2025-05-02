#Expression is array slice step with H only in projection/predicate and is not null in predicate
select id, s.address.phones[:4] from sn s where s.address.phones[:4] is not null