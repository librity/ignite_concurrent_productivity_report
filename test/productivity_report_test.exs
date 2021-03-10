defmodule ProductivityReportTest do
  use ExUnit.Case

  describe "build/1" do
    test "should genrate a report from raw .csv data" do
      file_name = "test_report"
      return = ProductivityReport.build(file_name)

      expected = %{
        all_hours: %{cleiton: 1, daniele: 12, giuliano: 9, jakeliny: 14, joseph: 3, mayk: 5},
        hours_per_month: %{
          cleiton: %{june: 1},
          daniele: %{april: 7, december: 5},
          giuliano: %{february: 9},
          jakeliny: %{july: 8, march: 6},
          joseph: %{march: 3},
          mayk: %{december: 5}
        },
        hours_per_year: %{
          cleiton: %{2020 => 1},
          daniele: %{2016 => 5, 2018 => 7},
          giuliano: %{2017 => 3, 2019 => 6},
          jakeliny: %{2017 => 8, 2019 => 6},
          joseph: %{2017 => 3},
          mayk: %{2017 => 1, 2019 => 4}
        }
      }

      assert return == expected
    end
  end

  describe "build_from_many/1" do
    test "should return an error when file_names isn't a string list" do
      return = ProductivityReport.buid_from_many("bad")
      expected = {:error, "Please provide a list of file names."}

      assert return == expected
    end
  end

  describe "fetch_max_from_field/2" do
    test "should return the greatest contributor from 'all_hours'" do
      file_name = "test_report"
      field = :all_hours

      return =
        file_name
        |> ProductivityReport.build()
        |> ProductivityReport.fetch_max_from_field(field)

      expected = {:ok, {:jakeliny, 14}}

      assert expected == return
    end

    test "should return an :error when field isn't valid" do
      file_name = "test_report"
      field = :foobar

      return =
        file_name
        |> ProductivityReport.build()
        |> ProductivityReport.fetch_max_from_field(field)

      expected = {:error, "Invalid field."}

      assert expected == return
    end
  end

  describe "fetch_max_from_deep_field/3" do
    test "should return the greatest contributor from 'hours_per_month'" do
      file_name = "test_report"
      field = :hours_per_month
      freelancer = :daniele

      return =
        file_name
        |> ProductivityReport.build()
        |> ProductivityReport.fetch_max_from_deep_field(field, freelancer)

      expected = {:ok, {:april, 7}}

      assert expected == return
    end

    test "should return the greatest contributor from 'hours_per_year'" do
      file_name = "test_report"
      field = :hours_per_year
      freelancer = :jakeliny

      return =
        file_name
        |> ProductivityReport.build()
        |> ProductivityReport.fetch_max_from_deep_field(field, freelancer)

      expected = {:ok, {2017, 8}}

      assert expected == return
    end

    test "should return an :error when field isn't valid" do
      file_name = "test_report"
      field = :foobar
      sub_field = :baz

      return =
        file_name
        |> ProductivityReport.build()
        |> ProductivityReport.fetch_max_from_deep_field(field, sub_field)

      expected = {:error, "Invalid field."}

      assert expected == return
    end
  end
end
