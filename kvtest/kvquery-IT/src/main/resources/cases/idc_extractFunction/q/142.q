#expression returns month using month()function for non json map
SELECT id,month(t.mts9.k0)as mts0,month(t.mts9.k1)as mts1,month(t.mts9.k2)as mts2,month(t.mts9.k3)as mts3,month(t.mts9.k4)as mts4,month(t.mts9.k5)as mts5,month(t.mts9.k6)as mts6,month(t.mts9.k7)as mts7 ,month(t.mts9.k8)as mts8 ,month(t.mts9.k9)as mts9 FROM Extract t
