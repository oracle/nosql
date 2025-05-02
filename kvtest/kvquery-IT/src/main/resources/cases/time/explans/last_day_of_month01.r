compiled-query-plan

{
"query file" : "time/q/last_day_of_month01.q",
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
      "target table" : "roundtest",
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
    "SELECT expressions" : [
      {
        "field name" : "last_day_t0",
        "field expression" : 
        {
          "iterator kind" : "FN_LAST_DAY_OF_MONTH",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t0",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "last_day_t3",
        "field expression" : 
        {
          "iterator kind" : "FN_LAST_DAY_OF_MONTH",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "last_day_l3",
        "field expression" : 
        {
          "iterator kind" : "FN_LAST_DAY_OF_MONTH",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "last_day_jl3",
        "field expression" : 
        {
          "iterator kind" : "FN_LAST_DAY_OF_MONTH",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "last_day_js6",
        "field expression" : 
        {
          "iterator kind" : "FN_LAST_DAY_OF_MONTH",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "s6",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
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