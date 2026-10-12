let Source = OData.Feed("https://svc.example.com/odata", null, [Implementation="2.0"]), t = Source{[Name="Orders"]}[Data] in t
