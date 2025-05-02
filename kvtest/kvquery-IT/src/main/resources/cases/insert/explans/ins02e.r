compiled-query-plan

{
"query file" : "insert/q/ins02e.q",
"plan" : 
{
  "iterator kind" : "INSERT_ROW",
  "row to insert (potentially partial)" : 
{
  "str" : "gtm",
  "id1" : 1,
  "id2" : 302
},
  "column positions" : [ 3, 4 ],
  "value iterators" : [
    {
      "iterator kind" : "CAST",
      "target type" : "Json",
      "quantifier" : "?",
      "input iterator" :
      {
        "iterator kind" : "EXTERNAL_VAR_REF",
        "variable" : "$info"
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
            "iterator kind" : "CONST",
            "value" : 120
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
            "iterator kind" : "FUNC_PARSE_JSON",
            "input iterator" :
            {
              "iterator kind" : "PROMOTE",
              "target type" : "String",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$recinfo2"
              }
            }
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
    }
  ]
}
}