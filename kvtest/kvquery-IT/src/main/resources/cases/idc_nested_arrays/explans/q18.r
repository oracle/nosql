compiled-query-plan

{
"query file" : "idc_nested_arrays/q/q18.q",
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
      "index used" : "idx_age_areacode_kind",
      "covering index" : false,
      "index row variable" : "$$nt_idx",
      "index scans" : [
        {
          "equality conditions" : {"age":17},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"age":30},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
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
          "value" : "home"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$nt",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "kind",
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "predicate iterator" :
          {
            "iterator kind" : "IN",
            "left-hand-side expressions" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "number",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              }
            ],
            "right-hand-side expressions" : [
              {
                "iterator kind" : "CONST",
                "value" : 31
              },
              {
                "iterator kind" : "CONST",
                "value" : 50
              },
              {
                "iterator kind" : "CONST",
                "value" : 60
              }
            ]
          },
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "phones",
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
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "home"
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