#atan with n that does not resolve to a number
select atan(t.doc.str), atan(t.doc.bool),atan(t.doc.obj) from functional_test t where t.id=7