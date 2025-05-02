compiled-query-plan

{
"query file" : "idc_maths/q/idx_atan2_dv.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "functional_test",
        "row variable" : "$$t",
        "index used" : "idx_atan2_dv",
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
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "ATAN2",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "atan2#dv@,3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t_idx"
                }
              },
              {
                "iterator kind" : "CONST",
                "value" : 3
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0.0
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
        },
        {
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "ATAN2",
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "atan2#dv@,3",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t_idx"
                }
              },
              {
                "iterator kind" : "CONST",
                "value" : 3
              }
            ]
          }
        }
      ]
    }
  }
}
}