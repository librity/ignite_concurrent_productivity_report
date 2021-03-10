defmodule ProductivityReport do
  alias ProductivityReport.Parser

  @shallow_fields [:all_hours]

  @deep_fields [:hours_per_month, :hours_per_year]

  def buid_from_many(file_names) when not is_list(file_names) do
    {:error, "Please provide a list of file names."}
  end

  def build_from_many(file_names) do
    report =
      file_names
      |> Task.async_stream(&build/1)
      |> Enum.reduce(initialize_report(), &merge_reports/2)

    {:ok, report}
  end

  def build(file_name) do
    file_name
    |> Parser.call()
    |> Enum.reduce(initialize_report(), &accumulate_fields/2)
  end

  def fetch_max_from_field(report, field) when field in @shallow_fields do
    {:ok, Enum.max_by(report[field], fn {_key, value} -> value end)}
  end

  def fetch_max_from_field(_report, _field), do: {:error, "Invalid field."}

  def fetch_max_from_deep_field(report, field, sub_field)
      when field in @deep_fields do
    {:ok, Enum.max_by(report[field][sub_field], fn {_key, value} -> value end)}
  end

  def fetch_max_from_deep_field(_report, _field, _sub_field), do: {:error, "Invalid field."}

  defp merge_reports({:ok, partial}, report) do
    all_hours = merge_all_hours(partial.all_hours, report.all_hours)
    hours_per_month = merge_hours_per_month(partial.hours_per_month, report.hours_per_month)
    hours_per_year = merge_hours_per_year(partial.hours_per_year, report.hours_per_year)

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp merge_all_hours(partial_all_hours, all_hours) do
    merge_by_value(partial_all_hours, all_hours)
  end

  defp merge_hours_per_month(partial_hours_per_month, hours_per_month) do
    merge_by_map(partial_hours_per_month, hours_per_month)
  end

  defp merge_hours_per_year(partial_hours_per_year, hours_per_year) do
    merge_by_map(partial_hours_per_year, hours_per_year)
  end

  defp merge_by_map(map_one, map_two) do
    Map.merge(map_one, map_two, fn _key, value_one, value_two ->
      merge_by_value(value_one, value_two)
    end)
  end

  defp merge_by_value(map_one, map_two) do
    Map.merge(map_one, map_two, fn _key, value_one, value_two -> value_one + value_two end)
  end

  defp accumulate_fields(line, report) do
    all_hours = accumulate_all_hours(line, report.all_hours)
    hours_per_month = accumulate_hours_per_month(line, report.hours_per_month)
    hours_per_year = accumulate_hours_per_year(line, report.hours_per_year)

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp accumulate_all_hours([freelancer, hours | _tails], all_hours) do
    previous = Map.get(all_hours, freelancer, 0)

    Map.put(all_hours, freelancer, previous + hours)
  end

  defp accumulate_hours_per_month([freelancer, hours, _day, month, _year], hours_per_month) do
    previous_freelancer = Map.get(hours_per_month, freelancer, %{})
    previous_month = Map.get(previous_freelancer, month, 0)
    new_month = Map.put(previous_freelancer, month, previous_month + hours)

    Map.put(hours_per_month, freelancer, new_month)
  end

  defp accumulate_hours_per_year([freelancer, hours, _day, _month, year], hours_per_year) do
    previous_freelancer = Map.get(hours_per_year, freelancer, %{})
    previous_year = Map.get(previous_freelancer, year, 0)
    new_year = Map.put(previous_freelancer, year, previous_year + hours)

    Map.put(hours_per_year, freelancer, new_year)
  end

  defp initialize_report, do: %{all_hours: %{}, hours_per_month: %{}, hours_per_year: %{}}

  defp build_report(all_hours, hours_per_month, hours_per_year) do
    %{all_hours: all_hours, hours_per_month: hours_per_month, hours_per_year: hours_per_year}
  end
end
