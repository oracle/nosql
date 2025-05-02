compiled-query-plan

{
"query file" : "insert/q/ins07e.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "str" : "abc",
  "id2" : 307,
  "info" : null,
  "rec1" : null
},
    "column positions" : [ 0, 4, 1 ],
    "value iterators" : [
      {
        "iterator kind" : "CAST",
        "target type" : "String",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$str2"
        }
      },
      {
        "iterator kind" : "CAST",
        "target type" : { "Record" : {
            "long" : "Long",
            "rec2" : 
              { "Record" : {
                  "str" : "String",
                  "num" : "Number",
                  "arr" : 
                    { "Array" : "Integer" }
                }
              },
            "recinfo" : 
              "Json",
            "map" : 
              { "Map" : "String" }
          }
        },
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "long"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$long"
            },
            {
              "iterator kind" : "CONST",
              "value" : "rec2"
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "str"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "dfg"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "num"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2300.0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "arr"
                },
                {
                  "iterator kind" : "ARRAY_CONSTRUCTOR",
                  "conditional" : false,
                  "input iterators" : [
                    {
                      "iterator kind" : "CONST",
                      "value" : 10
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 4
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 6
                    }
                  ]
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : "recinfo"
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [

              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : "map"
            },
            {
              "iterator kind" : "MAP_CONSTRUCTOR",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : "foo"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "xyz"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "bar"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : "dff"
                }
              ]
            }
          ]
        }
      },
      {
        "iterator kind" : "CAST",
        "target type" : "Integer",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$id1"
        }
      }
    ]
  },
  "FROM variable" : "$$foo",
  "SELECT expressions" : [
    {
      "field name" : "foo",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$$foo"
      }
    }
  ]
}
}