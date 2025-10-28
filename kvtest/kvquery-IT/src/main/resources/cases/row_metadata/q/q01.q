#
# partial key and range, plus always-true preds
#
select id, $t.info.age
from foo $t
where row_metadata($t).address.state = "CA" and
      row_metadata($t).address.city > "F" and
      row_metadata($t).address.city > "G"
