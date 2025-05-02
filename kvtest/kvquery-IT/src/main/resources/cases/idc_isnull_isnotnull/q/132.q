##Expression is array filter step with $element implicit variable with is not null in projection
select id, s.address.phones[$element.work=504].home is not null from sn s 