compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_neg08.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_trim_name",
      "covering index" : true,
      "index row variable" : "$$f_idx",
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
          "iterator kind" : "FN_TRIM",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#name",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "invalid_where_arg"
            }
          ]
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : null
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
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
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}