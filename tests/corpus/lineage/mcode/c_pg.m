let
 S = PostgreSQL.Database("pg.example.com:5432", "db"),
 t = S{[Schema="public",Item="orders"]}[Data]
in
 t
