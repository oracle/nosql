#Expression has is of type() operator with is null in projection
select id,s.children."B.Balance" is null from sn s where s.children."B.Balance" is of type (integer)