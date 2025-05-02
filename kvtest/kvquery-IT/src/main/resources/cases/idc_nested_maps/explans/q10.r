compiled-query-plan

{
"query file" : "idc_nested_maps/q/q10.q",
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
      "index used" : "idx_keys_number_kind",
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
          "field name" : "addresses.values().phones.values().values().number",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$nt_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 61
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
          "iterator kind" : "AND",
          "input iterators" : [
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
                "value" : 118
              }
            },
            {
              "iterator kind" : "ANY_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "number",
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
                "value" : 61
              }
            }
          ]
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
                "value" : "Portland"
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