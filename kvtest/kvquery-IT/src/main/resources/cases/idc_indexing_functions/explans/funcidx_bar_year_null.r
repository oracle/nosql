compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_bar_year_null.q",
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
      "index used" : "idx_year_month_day",
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
          "iterator kind" : "CONST",
          "value" : null
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 2021
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
      }
    ]
  }
}
}