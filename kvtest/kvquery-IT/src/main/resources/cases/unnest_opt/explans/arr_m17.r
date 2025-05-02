compiled-query-plan

{
"query file" : "unnest_opt/q/arr_m17.q",
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
      "index used" : "idx_state_city_age",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"address.state":"CA"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t",
    "FROM" :
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
              "iterator kind" : "VAR_REF",
              "variable" : "$t"
            }
          }
        }
      }
    },
    "FROM variable" : "$areacode",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$areacode"
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 650
          }
        },
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$areacode"
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 408
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
            "variable" : "$t"
          }
        }
      },
      {
        "field name" : "areacode",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$areacode"
        }
      }
    ]
  }
}
}