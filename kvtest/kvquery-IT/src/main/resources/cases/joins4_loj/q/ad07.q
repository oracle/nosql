# SR#26844 - https://sleepycat-tools.us.oracle.com/trac/ticket/26844
select *
from A.G g left outer join A a on g.ida1 = a.ida1
           left outer join A.G.H h on g.ida1 = h.ida1 and g.idg1 = h.idg1 and
                                 (h.idh1 = -2.34E7777N or h.idh2 = 7.55E+9999N)
