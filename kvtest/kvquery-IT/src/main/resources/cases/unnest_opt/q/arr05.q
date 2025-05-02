#
# Fake any pred. It is actually treated as eq.
# TODO: convert fake "any" preds to value-comp preds
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $phone.number
from foo as $t, $t.address.phones[] as $phone
where $t.address.state = "CA" and
      600 <any $phone.areacode and $phone.areacode <=any 620
