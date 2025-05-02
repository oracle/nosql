compiled-query-plan

{
"query file" : "time/q/timestamp_trunc_idx03.q",
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
      "index used" : "idx_trunc_s9_day",
      "covering index" : true,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "timestamp_trunc#s9@,'day'",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 2021
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t_idx",
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
            "variable" : "$$t_idx"
          }
        }
      }
    ]
  }
}
}