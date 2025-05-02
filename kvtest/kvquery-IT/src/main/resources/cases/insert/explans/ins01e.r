compiled-query-plan

{
"query file" : "insert/q/ins01e.q",
"plan" : 
{
  "iterator kind" : "INSERT_ROW",
  "row to insert (potentially partial)" : 
{
  "id1" : 1,
  "id2" : 301,
  "info" : {
    "a" : 10,
    "b" : 30
  }
},
  "column positions" : [ 0, 4 ],
  "value iterators" : [
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$str1"
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
    }
  ]
}
}