#sin of array types and array in json
select t.numArr, sin(t.numArr) as sinnumArr,
       t.num2DArr, sin(t.num2DArr) as sinnum2DArr,
       t.num3DArr, sin(t.num3DArr) as sinnum3DArr,
       t.douArr, sin(t.numArr) as sindouArr,
       t.dou2DArr, sin(t.num2DArr) as sindou2DArr,
       t.dou3DArr, sin(t.num3DArr) as sindou3DArr,
                     t.doc.numArr as docnumArr, sin(t.doc.numArr) as sindocnumArr,
                     t.doc.num2DArr as docnum2DArr, sin(t.doc.num2DArr) as sindocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, sin(t.doc.num3DArr) as sindocnum3DArr,
                     t.doc.douArr as docdouArr, sin(t.doc.numArr) as sindocdouArr,
                     t.doc.dou2DArr as docdou2DArr, sin(t.doc.num2DArr) as sindocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, sin(t.doc.num3DArr) as sindocdou3DArr
 from functional_test t where id=1