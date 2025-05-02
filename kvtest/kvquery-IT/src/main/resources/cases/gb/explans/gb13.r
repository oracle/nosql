compiled-query-plan

{
"query file" : "gb/q/gb13.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "tsminmax",
        "row variable" : "$$t",
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
      "FROM variable" : "$$t",
      "GROUP BY" : "No grouping expressions",
      "SELECT expressions" : [
        {
          "field name" : "ts3min",
          "field expression" : 
          {
            "iterator kind" : "FN_MIN",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ts3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        },
        {
          "field name" : "ts3max",
          "field expression" : 
          {
            "iterator kind" : "FN_MAX",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ts3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        },
        {
          "field name" : "mts6min",
          "field expression" : 
          {
            "iterator kind" : "FN_MIN",
            "input iterator" :
            {
              "iterator kind" : "FN_SEQ_MIN",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "mts6",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          }
        },
        {
          "field name" : "mts6max",
          "field expression" : 
          {
            "iterator kind" : "FN_MAX",
            "input iterator" :
            {
              "iterator kind" : "FN_SEQ_MAX",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "mts6",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "ts3min",
      "field expression" : 
      {
        "iterator kind" : "FN_MIN",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ts3min",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "ts3max",
      "field expression" : 
      {
        "iterator kind" : "FN_MAX",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ts3max",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "mts6min",
      "field expression" : 
      {
        "iterator kind" : "FN_MIN",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "mts6min",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "mts6max",
      "field expression" : 
      {
        "iterator kind" : "FN_MAX",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "mts6max",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}