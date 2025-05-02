# ancestor table alias conflicts with descendant table alias
select * from nested tables (A.B a ancestors (A t) descendants (A.B.C t))
