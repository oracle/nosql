compiled-query-plan

{
"query file" : "unnest_json/q/arr13.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$t",
      "index used" : "idx_state_areacode_kind",
      "covering index" : false,
      "index row variable" : "$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.address.state":"CA"},
          "range conditions" : { "info.address.phones[].areacode" : { "end value" : 620, "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t",
    "WHERE" : 
    {
      "iterator kind" : "ANY_LESS_THAN",
      "left operand" :
      {
        "iterator kind" : "CONST",
        "value" : 600
      },
      "right operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "areacode",
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
              "field name" : "address",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$t"
                }
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
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$t_idx"
          }
        }
      },
      {
        "field name" : "areacode",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "areacode",
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
                    "field name" : "address",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "info",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$t"
                      }
                    }
                  }
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}