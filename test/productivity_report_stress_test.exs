defmodule ProductivityReportStressTest do
  use ExUnit.Case

  describe "build/1" do
    test "should genrate a report from raw .csv data" do
      file_name = "gen_report"
      return = ProductivityReport.build(file_name)

      expected = %{
        all_hours: %{
          cleiton: 13_797,
          daniele: 13_264,
          giuliano: 13_671,
          jakeliny: 13_909,
          joseph: 13_174,
          mayk: 13_526,
          danilo: 13_583,
          diego: 13_015,
          rafael: 13_597,
          vinicius: 13_412
        },
        hours_per_month: %{
          cleiton: %{
            june: 1123,
            april: 1161,
            august: 1149,
            december: 1100,
            february: 1168,
            january: 1271,
            july: 1236,
            march: 1091,
            may: 998,
            november: 1266,
            october: 1073,
            september: 1161
          },
          daniele: %{
            april: 1138,
            december: 1019,
            august: 1018,
            february: 1003,
            january: 1130,
            july: 1135,
            june: 1092,
            march: 1177,
            may: 1170,
            november: 1131,
            october: 1046,
            september: 1205
          },
          giuliano: %{
            february: 1173,
            april: 1152,
            august: 1126,
            december: 1100,
            january: 1107,
            july: 1169,
            june: 1197,
            march: 1097,
            may: 1182,
            november: 1105,
            october: 1123,
            september: 1140
          },
          jakeliny: %{
            july: 1245,
            march: 1178,
            april: 1172,
            august: 1098,
            december: 1122,
            february: 1119,
            january: 1118,
            june: 1182,
            may: 1129,
            november: 1085,
            october: 1197,
            september: 1264
          },
          joseph: %{
            march: 1038,
            april: 1047,
            august: 1167,
            december: 1122,
            february: 1202,
            january: 1084,
            july: 1153,
            june: 1105,
            may: 1082,
            november: 1005,
            october: 1058,
            september: 1111
          },
          mayk: %{
            december: 1159,
            april: 1016,
            august: 1079,
            february: 1085,
            january: 1100,
            july: 1093,
            june: 1190,
            march: 1166,
            may: 1173,
            november: 1257,
            october: 1094,
            september: 1114
          },
          danilo: %{
            april: 1232,
            august: 1200,
            december: 1149,
            february: 1156,
            january: 1121,
            july: 1114,
            june: 966,
            march: 1096,
            may: 1075,
            november: 1122,
            october: 1237,
            september: 1115
          },
          diego: %{
            april: 1112,
            august: 1129,
            december: 1177,
            february: 1144,
            january: 1118,
            july: 1099,
            june: 1073,
            march: 1029,
            may: 1082,
            november: 995,
            october: 928,
            september: 1129
          },
          rafael: %{
            april: 1171,
            august: 1199,
            december: 965,
            february: 1042,
            january: 1125,
            july: 1097,
            june: 1218,
            march: 1220,
            may: 1141,
            november: 1163,
            october: 1161,
            september: 1095
          },
          vinicius: %{
            april: 1149,
            august: 1055,
            december: 1186,
            february: 1102,
            january: 1036,
            july: 979,
            june: 1213,
            march: 1191,
            may: 1109,
            november: 1169,
            october: 1113,
            september: 1110
          }
        },
        hours_per_year: %{
          cleiton: %{2020 => 2895, 2016 => 2699, 2017 => 2684, 2018 => 2805, 2019 => 2714},
          daniele: %{2016 => 2573, 2018 => 2633, 2017 => 2862, 2019 => 2562, 2020 => 2634},
          giuliano: %{2017 => 2696, 2019 => 2850, 2016 => 2784, 2018 => 2693, 2020 => 2648},
          jakeliny: %{2017 => 2625, 2019 => 2709, 2016 => 2869, 2018 => 2765, 2020 => 2941},
          joseph: %{2017 => 2617, 2016 => 2603, 2018 => 2587, 2019 => 2672, 2020 => 2695},
          mayk: %{2017 => 2735, 2019 => 2778, 2016 => 2644, 2018 => 2770, 2020 => 2599},
          danilo: %{2016 => 2746, 2017 => 2534, 2018 => 2972, 2019 => 2666, 2020 => 2665},
          diego: %{2016 => 2577, 2017 => 2647, 2018 => 2792, 2019 => 2486, 2020 => 2513},
          rafael: %{2016 => 2684, 2017 => 2570, 2018 => 2628, 2019 => 2786, 2020 => 2929},
          vinicius: %{2016 => 2530, 2017 => 2720, 2018 => 2654, 2019 => 2640, 2020 => 2868}
        }
      }

      assert return == expected
    end
  end
end
