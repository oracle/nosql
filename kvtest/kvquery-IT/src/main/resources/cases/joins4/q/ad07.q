# SR#26844 - https://sleepycat-tools.us.oracle.com/trac/ticket/26844
select * from nested tables (A.G g ancestors (A a) descendants(A.G.H h on h.idh1 = -2.34E7777N or h.idh2 = 7.55E+9999N))
