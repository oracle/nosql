compiled-query-plan

{
"query file" : "insert/q/ins15.q",
"plan" :
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" :
{
  "str" : "abc",
  "id1" : 100,
  "id2" : 1,
  "info" : null,
  "rec1" : {
    "long" : null,
    "rec2" : {
      "str" : null,
      "num" : 1E-500,
      "arr" : null
    },
    "recinfo" : {
      "val" : -1E-500
    },
    "map" : null
  }
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$f",
  "SELECT expressions" : [
    {
      "field name" : "num",
      "field expression" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "num",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rec2",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "rec1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        }
      }
    },
    {
      "field name" : "val",
      "field expression" :
      {
        "iterator kind" : "ARRAY_CONSTRUCTOR",
        "conditional" : true,
        "input iterators" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "val",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "recinfo",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "rec1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f"
                }
              }
            }
          }
        ]
      }
    }
  ]
}
}
