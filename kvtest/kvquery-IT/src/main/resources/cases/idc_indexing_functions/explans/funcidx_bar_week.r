compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar_week.q",
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
      "row variable" : "$$b",
      "index used" : "idx_week",
      "covering index" : true,
      "index row variable" : "$$b_idx",
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
          "iterator kind" : "FIELD_STEP",
          "field name" : "week#tm",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "CAST",
            "target type" : "Timestamp(0)",
            "quantifier" : "",
            "input iterator" :
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$week1"
            }
          }
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$b_idx",
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
            "variable" : "$$b_idx"
          }
        }
      },
      {
        "field name" : "week",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "week#tm",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$b_idx"
          }
        }
      }
    ]
  }
}
}