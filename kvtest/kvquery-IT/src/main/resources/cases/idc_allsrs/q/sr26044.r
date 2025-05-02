compiled-query-plan

{
"query file" : "idc_allsrs/q/sr26044.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "patient",
        "row variable" : "$$p",
        "index used" : "idx1",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "recordData.PATIENT_PRESCRIPTION_INFO.PRESCRIPTION_INFO[].PRESCRIPTION_DATE.\"@value\"" : { "start value" : "2016-9-2T00:00:00", "start inclusive" : true, "end value" : "2016-9-2T23:59:59", "end inclusive" : true } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$p",
      "SELECT expressions" : [
        {
          "field name" : "@value",
          "field expression" : 
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : true,
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "@value",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "PRESCRIPTION_DATE",
                  "input iterator" :
                  {
                    "iterator kind" : "ARRAY_FILTER",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "PRESCRIPTION_INFO",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "PATIENT_PRESCRIPTION_INFO",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "recordData",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$p"
                          }
                        }
                      }
                    }
                  }
                }
              }
            ]
          }
        },
        {
          "field name" : "id_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$p"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "@value",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "@value",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}