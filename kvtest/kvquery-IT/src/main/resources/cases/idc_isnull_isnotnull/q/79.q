#Expression has is of type() operator with is not null in projection
select id,s.children."B.Balance" is not null from sn s where s.children."B.Balance" is of type (long)