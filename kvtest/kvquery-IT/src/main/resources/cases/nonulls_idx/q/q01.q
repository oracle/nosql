#
# partial key and range, plus always-true preds
#
select id, t.info.age
from foo t
where t.info.address.state = "CA" and
      t.info.address.city > "F" and
      t.info.address.city > "G"
