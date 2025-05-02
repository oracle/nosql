compiled-query-plan

{
"query file" : "idc_new_timestamp/q/timestamp_trunc10.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
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
          "equality conditions" : {"id":2},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc",
    "SELECT expressions" : [
      {
        "field name" : "t0",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "t0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$roundFunc"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
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
              "value" : "iyear"
            }
          ]
        }
      },
      {
        "field name" : "t3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "t3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$roundFunc"
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "t3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$roundFunc"
            }
          }
        }
      },
      {
        "field name" : "s9",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "s9",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$roundFunc"
          }
        }
      },
      {
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "FN_TIMESTAMP_TRUNC",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s9",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$roundFunc"
            }
          }
        }
      }
    ]
  }
}
}