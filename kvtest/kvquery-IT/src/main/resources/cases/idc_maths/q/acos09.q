#acos of array types and array in json
select t.numArr, acos(t.numArr) as acosnumArr,
       t.num2DArr, acos(t.num2DArr) as acosnum2DArr,
       t.num3DArr, acos(t.num3DArr) as acosnum3DArr,
       t.douArr, acos(t.numArr) as acosdouArr,
       t.dou2DArr, acos(t.num2DArr) as acosdou2DArr,
       t.dou3DArr, acos(t.num3DArr) as acosdou3DArr,
                     t.doc.numArr as docnumArr, acos(t.doc.numArr) as acosdocnumArr,
                     t.doc.num2DArr as docnum2DArr, acos(t.doc.num2DArr) as acosdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, acos(t.doc.num3DArr) as acosdocnum3DArr,
                     t.doc.douArr as docdouArr, acos(t.doc.numArr) as acosdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, acos(t.doc.num2DArr) as acosdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, acos(t.doc.num3DArr) as acosdocdou3DArr
 from functional_test t where id=1