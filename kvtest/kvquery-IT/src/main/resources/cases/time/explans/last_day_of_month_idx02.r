compiled-query-plan

{
"query file" : "time/q/last_day_of_month_idx02.q",
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
      "target table" : "roundtest",
      "row variable" : "$$t",
      "index used" : "idx_js6_last_day_jl3_year",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"to_last_day_of_month#doc.s6":"2021-11-30T00:00:00Z"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "l3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 2024
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      }
    ]
  }
}
}