compiled-query-plan

{
"query file" : "json_idx/q/nex04.q",
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
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_areacode_kind",
      "covering index" : false,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "OP_NOT_EXISTS",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info.address.phones[].kind",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "ARRAY_FILTER",
        "predicate iterator" :
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "OP_NOT_EXISTS",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "kind",
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
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
              }
            },
            {
              "iterator kind" : "ANY_GREATER_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "areacode",
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
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 408
              }
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "address",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}