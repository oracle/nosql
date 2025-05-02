compiled-query-plan

{
"query file" : "map_index/q/upd18.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "UPDATE_ROW",
      "indexes to update" : [ "idx1_a_c_c_f", "idx2_ca_f_cb_cc_cd", "idx3_ca_f_cb_cc_cd", "idx4_c1_keys_vals_c3", "idx5_g_c_f", "idx6_c1_c2_c3", "idx7_c1_c3" ],
      "update clauses" : [
        {
          "iterator kind" : "REMOVE",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "KEYS",
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
              "equality conditions" : {"id":4},
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
    },
    "FROM variable" : "$$f",
    "SELECT expressions" : [
      {
        "field name" : "c",
        "field expression" : 
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
    ]
  }
}
}