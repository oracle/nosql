compiled-query-plan

{
"query file" : "map_index/q/upd08.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idx2_ca_f_cb_cc_cd", "idx3_ca_f_cb_cc_cd", "idx4_c1_keys_vals_c3", "idx6_c1_c2_c3" ],
    "update clauses" : [
      {
        "iterator kind" : "SET",
        "clone new values" : false,
        "theIsMRCounterDec" : false,
        "theJsonMRCounterColPos" : -1,
        "theIsJsonMRCounterUpdate" : false,
        "target iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "cb",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "c",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "rec",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$f"
                      }
                    }
                  }
                }
              }
            }
          }
        },
        "new value iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 200
        }
      }
    ],
    "update TTL" : false,
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$$f",
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
      "FROM variable" : "$$f",
      "SELECT expressions" : [
        {
          "field name" : "f",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      ]
    }
  }
}
}