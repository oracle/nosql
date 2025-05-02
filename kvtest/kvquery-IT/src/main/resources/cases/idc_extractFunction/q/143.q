#expression returns day using day()function for non json map
SELECT id,day(t.mts9.k0)as mts0,day(t.mts9.k1)as mts1,day(t.mts9.k2)as mts2,day(t.mts9.k3)as mts3,day(t.mts9.k4)as mts4,day(t.mts9.k5)as mts5,day(t.mts9.k6)as mts6,day(t.mts9.k7)as mts7 ,day(t.mts9.k8)as mts8 ,day(t.mts9.k9)as mts9 FROM Extract t
