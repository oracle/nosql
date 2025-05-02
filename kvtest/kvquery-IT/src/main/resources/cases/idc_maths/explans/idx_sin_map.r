compiled-query-plan

{
"query file" : "idc_maths/q/idx_sin_map.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
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
        "target table" : "functional_test",
        "row variable" : "$$m",
        "index used" : "idx_sin_map",
        "covering index" : false,
        "index row variable" : "$$m_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "index filtering predicate" :
        {
          "iterator kind" : "NOT_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "sin#numMap.values()",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$m_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0.0
          }
        },
        "position in join" : 0
      },
      "FROM variable" : "$$m",
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
              "variable" : "$$m"
            }
          }
        },
        {
          "field name" : "numMap",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "numMap",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$m"
            }
          }
        }
      ]
    }
  }
}
}