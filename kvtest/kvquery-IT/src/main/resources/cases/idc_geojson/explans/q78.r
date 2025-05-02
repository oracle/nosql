compiled-query-plan

{
"query file" : "idc_geojson/q/q78.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "points",
        "row variable" : "$$p",
        "index used" : "idx_kind_ptn",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9vy", "start inclusive" : true, "end value" : "t9vzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9yn", "start inclusive" : true, "end value" : "t9yrzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9yw", "start inclusive" : true, "end value" : "t9yzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9zn", "start inclusive" : true, "end value" : "t9zrzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9zw", "start inclusive" : true, "end value" : "t9zzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tcbn", "start inclusive" : true, "end value" : "tcbrzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tcbw", "start inclusive" : true, "end value" : "tcbxzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdjb", "start inclusive" : true, "end value" : "tdjczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdjf", "start inclusive" : true, "end value" : "tdjgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdju", "start inclusive" : true, "end value" : "tdjvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdjy", "start inclusive" : true, "end value" : "tdjzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmb", "start inclusive" : true, "end value" : "tdmczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmf", "start inclusive" : true, "end value" : "tdmgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmu", "start inclusive" : true, "end value" : "tdmvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmy", "start inclusive" : true, "end value" : "tdrzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdtb", "start inclusive" : true, "end value" : "tdtczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdtf", "start inclusive" : true, "end value" : "tdtgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdtu", "start inclusive" : true, "end value" : "tdtu", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdw0", "start inclusive" : true, "end value" : "tdwhzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdwk", "start inclusive" : true, "end value" : "tdwk", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdws", "start inclusive" : true, "end value" : "tdws", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdwu", "start inclusive" : true, "end value" : "tdwu", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdx0", "start inclusive" : true, "end value" : "tdxhzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdxk", "start inclusive" : true, "end value" : "tdxk", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdxs", "start inclusive" : true, "end value" : "tdxs", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdxu", "start inclusive" : true, "end value" : "tdxu", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf00", "start inclusive" : true, "end value" : "tf09zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf0d", "start inclusive" : true, "end value" : "tf0ezzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf0h", "start inclusive" : true, "end value" : "tf0tzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf0w", "start inclusive" : true, "end value" : "tf0xzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf20", "start inclusive" : true, "end value" : "tf29zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf2d", "start inclusive" : true, "end value" : "tf2ezzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf2h", "start inclusive" : true, "end value" : "tf2tzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf2w", "start inclusive" : true, "end value" : "tf2xzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf80", "start inclusive" : true, "end value" : "tf89zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf8d", "start inclusive" : true, "end value" : "tf8ezzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf8h", "start inclusive" : true, "end value" : "tf8h", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf8k", "start inclusive" : true, "end value" : "tf8k", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf8s", "start inclusive" : true, "end value" : "tf8s", "end inclusive" : true } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$p",
      "WHERE" : 
      {
        "iterator kind" : "FN_GEO_WITHIN_DISTANCE",
        "search target iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "point",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$p"
            }
          }
        },
        "search geometry iterator" :
        {
          "iterator kind" : "CONST",
          "value" : {"coordinates":[[[[77.50614166259766,12.966754416691947],[77.60295867919922,12.947348706524428],[77.61051177978516,12.908198108318507],[77.50511169433594,12.888787845039232],[77.55146026611328,12.879416834404825],[77.59712219238281,12.875735270042483],[77.65617370605469,12.901170428290447],[77.70492553710938,12.963743284940753],[77.67539978027344,13.033659039547265],[77.52056121826172,12.999874459369373],[77.50614166259766,12.966754416691947]]],[[[77.55935668945312,13.057740282292977],[77.52948760986328,13.065766841807562],[77.45155334472656,13.048375633247892],[77.4481201171875,13.016600074665284],[77.43438720703125,12.94701411205675],[77.46047973632811,12.962739566261234],[77.49000549316406,12.967758119178917],[77.50991821289062,12.982144076424275],[77.49378204345703,13.004892262383622],[77.52124786376953,13.032655603554861],[77.55935668945312,13.056402497014313],[77.52777099609375,13.002550633604786],[77.49412536621094,13.00355419150191],[77.55935668945312,13.057740282292977]]]],"type":"MultiPolygon"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 200000.098
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
              "variable" : "$$p"
            }
          }
        },
        {
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "GEO_DISTANCE",
            "first geometry iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "point",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
                }
              }
            },
            "second geometry iterator" :
            {
              "iterator kind" : "CONST",
              "value" : {"coordinates":[[[[77.50614166259766,12.966754416691947],[77.60295867919922,12.947348706524428],[77.61051177978516,12.908198108318507],[77.50511169433594,12.888787845039232],[77.55146026611328,12.879416834404825],[77.59712219238281,12.875735270042483],[77.65617370605469,12.901170428290447],[77.70492553710938,12.963743284940753],[77.67539978027344,13.033659039547265],[77.52056121826172,12.999874459369373],[77.50614166259766,12.966754416691947]]],[[[77.55935668945312,13.057740282292977],[77.52948760986328,13.065766841807562],[77.45155334472656,13.048375633247892],[77.4481201171875,13.016600074665284],[77.43438720703125,12.94701411205675],[77.46047973632811,12.962739566261234],[77.49000549316406,12.967758119178917],[77.50991821289062,12.982144076424275],[77.49378204345703,13.004892262383622],[77.52124786376953,13.032655603554861],[77.55935668945312,13.056402497014313],[77.52777099609375,13.002550633604786],[77.49412536621094,13.00355419150191],[77.55935668945312,13.057740282292977]]]],"type":"MultiPolygon"}
            }
          }
        }
      ]
    }
  }
}
}