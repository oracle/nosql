compiled-query-plan

{
"query file" : "idc_nested_arrays/q/q07.q",
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
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$nt",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "ANY_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "city",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
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
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "Boston"
          }
        },
        {
          "iterator kind" : "ANY_LESS_THAN",
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
                "iterator kind" : "FIELD_STEP",
                "field name" : "addresses",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$nt"
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 31
          }
        }
      ]
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