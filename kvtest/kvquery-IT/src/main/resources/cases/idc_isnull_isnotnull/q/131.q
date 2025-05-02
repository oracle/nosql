#Expression is array filter step with $element implicit variable with is null in projection
select id, s.address.phones[$element.work=504].home is null from sn s