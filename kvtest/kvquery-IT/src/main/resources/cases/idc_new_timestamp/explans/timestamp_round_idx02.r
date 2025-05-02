compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_round_idx02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "roundFunc",
      "row variable" : "$$roundFunc",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FN_TIMESTAMP_ROUND",
        "input iterators" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "t0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$roundFunc"
            }
          },
          {
            "iterator kind" : "CONST",
            "value" : "year"
          }
        ]
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "2022-01-01T00:00:00Z"
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
            "variable" : "$$roundFunc"
          }
        }
      },
      {
        "field name" : "t0_to_year",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_ROUND",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$roundFunc"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "year"
            }
          ]
        }
      }
    ]
  }
}
}