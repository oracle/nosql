compiled-query-plan

{
"query file" : "idc_new_timestamp/q/day_of_month03.q",
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
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "id" : { "end value" : 2, "end inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
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
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "t0",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
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
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "s9",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      },
      {
        "field name" : "t1",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t1",
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
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "PROMOTE",
            "target type" : "Any",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "t1",
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
          }
        }
      }
    ]
  }
}
}