#Expression is array filter step with $element implicit variable with is not null in predicate and id null in projection
select id, s.address.phones[$element.work=504].home is null from sn s where s.address.phones is not null
