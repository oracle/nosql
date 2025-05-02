compiled-query-plan

{
"query file" : "idc_nested_arrays/q/q10.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "nestedTable",
      "row variable" : "$$nt",
      "index used" : "idx_firstName_number_kind",
      "covering index" : true,
      "index row variable" : "$$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "ANY_EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "addresses[].phones[][].kind",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$nt_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "work"
            }
          },
          {
            "iterator kind" : "ANY_GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "addresses[].phones[][].number",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$nt_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 60
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$nt_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#Id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$nt_idx"
          }
        }
      }
    ]
  }
}
}