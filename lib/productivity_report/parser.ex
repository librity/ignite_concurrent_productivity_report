defmodule ProductivityReport.Parser do
  @month_to_atom %{
    "1" => :january,
    "2" => :february,
    "3" => :march,
    "4" => :april,
    "5" => :may,
    "6" => :june,
    "7" => :july,
    "8" => :august,
    "9" => :september,
    "10" => :october,
    "11" => :november,
    "12" => :december
  }

  def call(file_name) do
    "data/#{file_name}.csv"
    |> File.stream!()
    |> Stream.map(&parse_line/1)
  end

  defp parse_line(line) do
    line
    |> String.trim()
    |> String.split(",")
    |> List.update_at(0, &parse_freelancer/1)
    |> List.update_at(1, &parse_hours/1)
    |> List.update_at(2, &parse_day/1)
    |> List.update_at(3, &parse_month/1)
    |> List.update_at(4, &parse_year/1)
  end

  defp parse_freelancer(freelancer_name) do
    freelancer_name
    |> String.downcase()
    |> String.to_atom()
  end

  defp parse_hours(hours), do: String.to_integer(hours)

  defp parse_day(day), do: String.to_integer(day)
  defp parse_month(month), do: @month_to_atom[month]
  defp parse_year(year), do: String.to_integer(year)
end
