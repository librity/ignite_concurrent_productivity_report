# ConcurrentProductivityReport

A simple Elixir program that concurrently generates an employee productivity report from raw CSV data.

CSV colums are parsed as name, hours worked, day, month and year.

## Commands

```bash
$ mix deps.get
$ mix credo gen.config
$ mix credo
$ mix credo --strict
$ mix test
```

Generate reports:

```elixir
> ProductivityReport.build("test_report")
> ProductivityReport.build("gen_report")
> ProductivityReport.build_from_many(["part_1", "part_2", "part_3"])
```

Measuring execution time in microseconds:

```elixir
> :timer.tc(fn -> ProductivityReport.build("test_report") end)
> :timer.tc(fn -> ProductivityReport.build("gen_report") end)
> :timer.tc(fn -> ProductivityReport.build_from_many(["part_1", "part_2", "part_3"]) end)
```

Exploring Elixir's syntax:

```elixir
> %{a: 1, 2016: 4}
** (SyntaxError) iex:1:13: unexpected token: ":" (column 13, code point U+003A)
> %{a: 1, "2016": 4}
%{"2016": 4, a: 1}
> map = %{:a => 1, 2016 => 4}
%{2016 => 4, :a => 1}
> Map.get(map, 2016, :default)
4
> Map.get(map, 2017, :default)
:default
```

## Resources

- https://www.tutorialspoint.com/elixir/elixir_data_types.htm
- https://stackoverflow.com/questions/43630781/convert-integer-to-atom-in-elixir
- https://hexdocs.pm/elixir/master/Date.html
- https://hexdocs.pm/elixir/master/DateTime.html
