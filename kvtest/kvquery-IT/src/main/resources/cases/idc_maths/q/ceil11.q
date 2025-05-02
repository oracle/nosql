#ceil with n that does not resolve to a number
select ceil(t.doc.str), ceil(t.doc.bool),ceil(t.doc.obj) from functional_test t where t.id=7