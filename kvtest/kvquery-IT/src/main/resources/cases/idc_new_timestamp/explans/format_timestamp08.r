compiled-query-plan

{
"query file" : "idc_new_timestamp/q/format_timestamp08.q",
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
      "target table" : "jsonCollection_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "l3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "l3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-MM-dd"
            },
            {
              "iterator kind" : "CONST",
              "value" : "GMT+05:00"
            }
          ]
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-MM-dd'T'HH:mm:ssXXXXX"
            },
            {
              "iterator kind" : "CONST",
              "value" : "Asia/Kolkata"
            }
          ]
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "EEE, dd MMM yyyy HH:mm:ss zzzz"
            },
            {
              "iterator kind" : "CONST",
              "value" : "America/New_York"
            }
          ]
        }
      },
      {
        "field name" : "Column_5",
        "field expression" : 
        {
          "iterator kind" : "FN_FORMAT_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-DDD"
            },
            {
              "iterator kind" : "CONST",
              "value" : "UTC"
            }
          ]
        }
      }
    ]
  }
}
}