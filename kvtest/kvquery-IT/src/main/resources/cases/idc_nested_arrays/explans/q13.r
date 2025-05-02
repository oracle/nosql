compiled-query-plan

{
"query file" : "idc_nested_arrays/q/q13.q",
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
      "covering index" : false,
      "index row variable" : "$$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "ANY_EQUAL",
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
          "value" : 50
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$nt",
    "WHERE" : 
    {
      "iterator kind" : "ANY_GREATER_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "number",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "OP_EXISTS",
              "input iterator" :
              {
                "iterator kind" : "ARRAY_FILTER",
                "predicate iterator" :
                {
                  "iterator kind" : "ANY_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "number",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$element"
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 50
                  }
                },
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "phones",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              }
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "addresses",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$nt"
              }
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 60
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
            "variable" : "$$nt"
          }
        }
      }
    ]
  }
}
}