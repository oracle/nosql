#exp of array types and array in json
select t.numArr, exp(t.numArr) as expnumArr,
       t.num2DArr, exp(t.num2DArr) as expnum2DArr,
       t.num3DArr, exp(t.num3DArr) as expnum3DArr,
       t.douArr, exp(t.numArr) as expdouArr,
       t.dou2DArr, exp(t.num2DArr) as expdou2DArr,
       t.dou3DArr, exp(t.num3DArr) as expdou3DArr,
                     t.doc.numArr as docnumArr, exp(t.doc.numArr) as expdocnumArr,
                     t.doc.num2DArr as docnum2DArr, exp(t.doc.num2DArr) as expdocnum2DArr,
                     t.doc.num3DArr as docnum3DArr, exp(t.doc.num3DArr) as expdocnum3DArr,
                     t.doc.douArr as docdouArr, exp(t.doc.numArr) as expdocdouArr,
                     t.doc.dou2DArr as docdou2DArr, exp(t.doc.num2DArr) as expdocdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, exp(t.doc.num3DArr) as expdocdou3DArr
 from functional_test t where id=1