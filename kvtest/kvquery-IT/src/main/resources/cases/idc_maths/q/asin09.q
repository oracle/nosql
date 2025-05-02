#asin of array types and array in json
select t.numArr, asin(t.numArr) as asinnumArr,
       t.num2DArr, asin(t.num2DArr) as asinnum2DArr,
       t.num3DArr, asin(t.num3DArr) as asinnum3DArr,
       t.douArr, asin(t.numArr) as asindouArr,
       t.dou2DArr, asin(t.num2DArr) as asindou2DArr,
       t.dou3DArr, asin(t.num3DArr) as asindou3DArr,
                     t.doc.numArr as docnumArr, asin(t.doc.numArr) as asindocnumArr,
                     t.doc.num2DArr as docnum2DArr, asin(t.doc.num2DArr) as asindocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, asin(t.doc.num3DArr) as asindocnum3DArr,
                     t.doc.douArr as docdouArr, asin(t.doc.numArr) as asindocdouArr,
                     t.doc.dou2DArr as docdou2DArr, asin(t.doc.num2DArr) as asindocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, asin(t.doc.num3DArr) as asindocdou3DArr
 from functional_test t where id=1