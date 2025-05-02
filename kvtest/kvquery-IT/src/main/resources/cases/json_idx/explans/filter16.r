compiled-query-plan

{
"query file" : "json_idx/q/filter16.q",
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
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.address.phones[].kind",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "home"
            }
          },
          {
            "iterator kind" : "OR",
            "input iterators" : [
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info.address.phones[].areacode",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 415
                }
              },
              {
                "iterator kind" : "GREATER_THAN",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info.address.phones[].areacode",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 650
                }
              }
            ]
          }
        ]
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