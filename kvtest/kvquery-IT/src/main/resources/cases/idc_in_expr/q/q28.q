select /*+ FORCE_PRIMARY_INDEX(ComplexType) */
      id
from ComplexType f
where lng in (f.children.values().age)
