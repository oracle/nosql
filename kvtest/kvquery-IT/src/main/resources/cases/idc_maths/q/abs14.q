#abs of array types on NaN and nulls and in json
select t.numArr, abs(t.numArr) as absnumArr,
              t.num2DArr, abs(t.num2DArr) as absnum2DArr,
              t.num3DArr, abs(t.num3DArr) as absnum3DArr,
              t.douArr, abs(t.numArr) as absdouArr,
              t.dou2DArr, abs(t.num2DArr) as absdou2DArr,
              t.dou3DArr, abs(t.num3DArr) as absdou3DArr,
                            t.doc.numArr as docnumArr, abs(t.doc.numArr) as absdocnumArr,
                            t.doc.num2DArr as docnum2DArr, abs(t.doc.num2DArr) as absdocnum2DArr,
                            t.doc.num3DArr as docnum3DArr, abs(t.doc.num3DArr) as absdocnum3DArr,
                            t.doc.douArr as docdouArr, abs(t.doc.numArr) as absdocdouArr,
                            t.doc.dou2DArr as docdou2DArr, abs(t.doc.num2DArr) as absdocdou2DArr,
                            t.doc.dou3DArr as  docdou3DArr, abs(t.doc.num3DArr) as absdocdou3DArr
 from functional_test t where id=4
