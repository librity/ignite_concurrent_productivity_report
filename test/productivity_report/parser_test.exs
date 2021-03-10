defmodule ProductivityReport.ParserTest do
  use ExUnit.Case

  alias ProductivityReport.Parser

  describe "call/1" do
    test "should read file and return a list of parsed contents" do
      file_name = "test_report"

      return =
        file_name
        |> Parser.call()
        |> Enum.map(& &1)

      expected = [
        [:daniele, 7, 29, :april, 2018],
        [:mayk, 4, 9, :december, 2019],
        [:daniele, 5, 27, :december, 2016],
        [:mayk, 1, 2, :december, 2017],
        [:giuliano, 3, 13, :february, 2017],
        [:cleiton, 1, 22, :june, 2020],
        [:giuliano, 6, 18, :february, 2019],
        [:jakeliny, 8, 18, :july, 2017],
        [:joseph, 3, 17, :march, 2017],
        [:jakeliny, 6, 23, :march, 2019]
      ]

      assert return == expected
    end
  end
end
