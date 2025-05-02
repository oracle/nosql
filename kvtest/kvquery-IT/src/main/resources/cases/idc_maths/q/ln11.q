#ln with n that does not resolve to a number
select ln(t.doc.str), ln(t.doc.bool),ln(t.doc.obj) from functional_test t where t.id=7