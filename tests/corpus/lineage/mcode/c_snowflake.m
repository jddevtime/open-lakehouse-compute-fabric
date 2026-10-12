let
 S = Snowflake.Databases("acc.snowflakecomputing.com","WH"),
 d = S{[Name="DB"]}[Data],
 s = d{[Name="PUBLIC",Kind="Schema"]}[Data],
 t = s{[Name="T",Kind="Table"]}[Data]
in
 t
