#
# partial key and range, plus always-true preds
#
select id, age
from foo t
where t.address.state = "CA" and
      t.address.city > "F" and
      t.address.city > "G"
