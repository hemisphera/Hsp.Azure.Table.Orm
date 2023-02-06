# A simple ORM (Object-Relational Mapper) for Azure Table Storage

## Getting started

- Create a class for each table you want to use. Your class does not need to descend from a specific base class.
- Decorate the properties you want to use as fields in your table with one of the following attributes:
    - `[Partitionkey]`: Denotes the property to use as the tables `PartitionKey`. This is required. Almost. There are cases where that's not true. See below.
    - `[RowKey]`: Denotes the property to use as the tables `RowKey`. This is required.
    - `[Field]`: "normal" table fields. You can specify a different name that the field should have on the storage. If you don't, the property name is used as the field name.
- Register your class through `TableMetadata.Register<T>(string name)` and make it known to the library. `name` specifies the table name on the storage.
- The `Register` method allows you to also specify a 'fixed partition key' for the table. This is helpful if you have a table with only a few records and only one key property. If you use a 'fixed partition key', then you do not need to provide your class with [PartitionKey] attribute, because it's partition key is implicit.

## Usage

Consider this class:

````
public class BassGuitar
{
    [RowKey] public string Name { get; set;}
    [PartitionKey] public string Manufacturer { get; set;}
    [Field] public int NoOfStrings { get; set;}
}

TableMetadata.Register<BassGuitar>("basses");
````

You can then insert a new record like so:

````
var sr1005 = new BassGuitar() {
    Name = "SR 5006OL",
    Manufacturer = "Ibanez",
    NoOfStrings = 6
}
Record.Open<BassGuitar>()
    .Store(sr1005);
````

And you can query it like so:

````
var items = await Record.Open<BassGuitar>()
    // set a filter for manufacturer = 'Ibanez'
    .SetRange(a => a.Manufacturer, "Ibanez) 
    // set a filter for at least 5 strings
    .SetRange(a => a.NoOfString, 5, QueryComparisons.GreaterThanOrEqual)
    .Read();
````
