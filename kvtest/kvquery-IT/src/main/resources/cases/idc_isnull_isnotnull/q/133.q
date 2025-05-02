#Expression is array filter step with $element implicit variable with is null in predicate
select id, s.address.phones[$element.work=504].home  from sn s where s.address.phones is null