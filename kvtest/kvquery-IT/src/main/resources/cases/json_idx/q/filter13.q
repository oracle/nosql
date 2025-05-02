#
# TODO: in this case, the pred factors :
# 408 < $element.areacode and $elment.areacode <= 650
# should be in the same pred group, but the algorithm does not
# recognize this
#
select /* FORCE_INDEX(Foo idx_areacode_kind) */id
from foo f
where f.info.address[exists $element.phones[408 < $element.areacode and
                                            $element.areacode <= 650]
                    ].phones.areacode >any 510
