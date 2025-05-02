compiled-query-plan

{
"query file" : "time/q/ext_tm10.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "bar",
      "row variable" : "$$bar",
      "index used" : "idx_tm",
      "covering index" : true,
      "index row variable" : "$$bar_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "tm" : { "start value" : "-6384-00-00T00:00:00.000Z", "start inclusive" : true } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "CAST",
          "target type" : "Timestamp(3)",
          "quantifier" : "",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$tm4"
          }
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$bar_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$bar_idx"
          }
        }
      },
      {
        "field name" : "tm",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "tm",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$bar_idx"
          }
        }
      }
    ]
  }
}
}