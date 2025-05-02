compiled-query-plan

{
"query file" : "idc_nested_maps/q/q05.q",
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
      "row variable" : "$nt",
      "index used" : "idx_age_areacode_kind",
      "covering index" : false,
      "index row variable" : "$nt_idx",
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
          "field name" : "addresses.values().phones.values().values().areacode",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$nt_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 408
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$nt",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "VALUES",
        "predicate iterator" :
        {
          "iterator kind" : "ANY_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "areacode",
            "input iterator" :
            {
              "iterator kind" : "VALUES",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$value"
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 408
          }
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "VALUES",
            "predicate iterator" :
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "city",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$value"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : "Santa Cruz"
              }
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "addresses",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$nt"
              }
            }
          }
        }
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
            "variable" : "$nt"
          }
        }
      }
    ]
  }
}
}