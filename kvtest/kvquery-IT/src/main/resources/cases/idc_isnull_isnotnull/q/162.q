#Expression is array slice step with L only in projection and is not null in projection
select id,s.children.Fred.Relative.Uncle[1:] is not null from sn s