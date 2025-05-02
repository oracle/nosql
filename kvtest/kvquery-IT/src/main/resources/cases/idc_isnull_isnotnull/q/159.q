#Expression is array slice step with L only in projection and is null in projection
select id,s.children.Fred.Relative.Uncle[1:] is null from sn s where s.children.Fred.Relative.Uncle[1:]