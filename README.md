# ElasticConnection

This is a minimum implementation of .Net DbConnection, DbCommand and DbDatareader
to connect, read, parse and bulk load data to Elasticsearch.


### Creating a connection       
    
    string query =  @"POST /someindex/_search
                      {
                        "size":100,
                        "query":{ "term": {"field1": "searchValue" }}
                      }";

    var connection = new ElasticDbConnection("Server=http://localhost:9200", new []  { typeof(HitsParser) });
    var command    = new ElasticDbCommand(query);

### Read data using the datareader
    
    using(var dataReader = new ElasticDbDataReader(command))
    {
        var fieldNames = dataReader.FieldNames;
        while(dataReader.Read())
        {
            // read field by field
            var fieldByField = Enumerable.Range(0, dataReader.FieldCount).Select(i => dataReader.GetValue(i)).ToArray();
            
            // or read the entire row 
            object[] rowAtOnce = new object[dataReader.FieldCount];
            dataReader.GetValues(out rowAtOnce);
        }
    }

### Read data using Dapper

    var data = connection.Query<HitsType>(query);

### Using multiple ResultParsers

    var connection = new ElasticDbConnection("Server=http://localhost:9200", new []  { typeof(HitsParser), typeof(SearchResultParser) });
    var multi = conn.QueryMultiple(query);

    var hits = multi.Read<HitsType>();
    var searchResult = multi.Read<SearchResult>();

### Available ResultParsers
- **SearchResultParser** reads the result information fields  
    - Took
    - TimedOut
    - ShardsTotal
    - ShardsSuccessful
    - ShardsFailed
    - TotalHits 

- **HitsResultParser** reads the hits collection (nested objects not supported)

- **AggregationParser** reads the aggregation collection

### Creating a custom ResultParser

    public class CustomParser : ResultParser
    {
        public CustomParser(string json) : base(json)
        {
            // The json string contains the data that is received from Elasticsearch
        }

        public override object GetValue(int ordinal)
        {
            // current row index is available in base._position, or override MoveNext to keep your own counter
            // return the value for row _position and fieldindex ordrinal
            ...
        }
    }

to use it, pass the type to the connection
    
    var connection = new ElasticDbConnection("Server=http://localhost:9200", new [] { typeof(CustomParser) });

### Bulk loading data

    // Open a bulk stream
    var streamWriter = conn.BeginBulkLoad();
   
    // write bulk commands, see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
    streamWriter.WriteLine(...);

    // When done writing commands call EndBulkLoad
    // the data will be POSTed to http://server:9200/_bulk
    // the results (if required) will contain the parsed results from elasticsearch, not suitable for large bulk loads (+10k)
    var results = conn.EndBulkLoad(streamWriter, true);
   