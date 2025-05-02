compiled-query-plan

{
"query file" : "insert/q/ins02r.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "str" : "gtm",
  "id1" : 1,
  "id2" : 302,
  "rec1" : {
    "long" : 120,
    "rec2" : {
      "str" : "dfg",
      "num" : 2.3E+3,
      "arr" : [10, 4, 6]
    },
    "recinfo" : {
      "a" : 10
    },
    "map" : {
      "bar" : "dff",
      "foo" : "xyz"
    }
  }
},
    "column positions" : [ 3 ],
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
      }
    ]
  },
  "FROM variable" : "$$f",
  "SELECT expressions" : [
    {
      "field name" : "info",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "info",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$f"
        }
      }
    },
    {
      "field name" : "rec1",
      "field expression" : 
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
  ]
}
}