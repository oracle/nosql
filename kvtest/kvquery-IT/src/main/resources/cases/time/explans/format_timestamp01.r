compiled-query-plan

{
"query file" : "time/q/format_timestamp01.q",
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
      "target table" : "arithtest",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
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
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "d0",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "tm9",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "d1",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm9",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "MMMdd yyyyVV"
            }
          ]
        }
      },
      {
        "field name" : "d2",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "PROMOTE",
            "target type" : "Any",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm3",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
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
        "field name" : "d3",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tm3",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "HH-mm-ss.SSSSSS"
            }
          ]
        }
      },
      {
        "field name" : "d4",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "PROMOTE",
            "target type" : "Any",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "tm6",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
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
        "field name" : "d5",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tm6",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-ww'w'"
            }
          ]
        }
      },
      {
        "field name" : "d6",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "tm6",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "notexists",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "d7",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "PROMOTE",
              "target type" : "Any",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "notexists",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-MM-dd"
            }
          ]
        }
      }
    ]
  }
}
}