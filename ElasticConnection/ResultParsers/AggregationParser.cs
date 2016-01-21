/**
 * Copyright 2015 Kris Daniels.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

namespace ElasticConnection.ResultParsers
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    public class AggregationParser : ResultParser
    {
        private JObject _data;

        private List<Type> _fieldTypes = new List<Type>();
        private List<string> _fieldNames = new List<string>();
        Dictionary<int, object> _currentRecord = new Dictionary<int, object>();
        List<Dictionary<int, object>> _records = new List<Dictionary<int, object>>();

        private string lastSeenKeyName;
        private bool lastValueWasKey = true;

        public AggregationParser(string json) : base(json)
        {
            if (HasAggregations(json))
            {
                // TODO : Rewrite parsing to use JsonTextReader, for now leave it as is because the overhead is low on smaller aggregation result sets
                JObject root = JObject.Parse(json);
                _data = root;
                ParseAggregations(_data["aggregations"]);
                // add last processed record to records
                _records.Add(_currentRecord);
                Length = _records.Count;

                FieldNames = _fieldNames.ToArray();
                FieldTypes = _fieldTypes.ToArray();
            }
            else
            {
                FieldNames = new[] { "dummy" };
                FieldTypes = new[] { typeof(string) };
            }
        }

        private static bool HasAggregations(string json)
        {
            using (var reader = new StringReader(json))
            using (var jsonReader = new JsonTextReader(reader))
            {
                while (jsonReader.Read())
                {
                    if (jsonReader.Depth > 1) continue;
                    if (jsonReader.TokenType == JsonToken.PropertyName && string.CompareOrdinal((string)jsonReader.Value, "aggregations") == 0)
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        public override object GetValue(int ordinal)
        {
            return _records[_position][ordinal];
        }

        private void ParseAggregations(JToken jToken)
        {
            foreach (var child in jToken.Children())
            {
                if (child is JProperty)
                {
                    JProperty prop = (JProperty)child;
                    if (
                       string.CompareOrdinal(prop.Name, "doc_count_error_upper_bound") != 0
                    && string.CompareOrdinal(prop.Name, "sum_other_doc_count") != 0
                    && string.CompareOrdinal(prop.Name, "doc_count") != 0
                    && string.CompareOrdinal(prop.Name, "value") != 0
                    && string.CompareOrdinal(prop.Name, "value_as_string") != 0
                    && string.CompareOrdinal(prop.Name, "buckets") != 0
                    )
                    {
                        var hasBuckets = prop.First.Children<JProperty>().Any(p => p.Name == "buckets");

                        if (prop.Name == "key" && lastValueWasKey == false)
                        {
                            //new record
                            _records.Add(_currentRecord);

                            // verrify if this is actually correct, as in a new value is always present even not present in data (as in null)
                            _currentRecord = new Dictionary<int, object>(_currentRecord);
                        }

                        if (prop.Name != "key")
                        {
                            //add field name and type
                            if (_fieldNames.Contains(prop.Name) == false)
                            {
                                _fieldNames.Add(prop.Name);
                                _fieldTypes.Add(GetAggFieldType(prop));
                            }

                            if (hasBuckets)
                            {
                                lastSeenKeyName = prop.Name;
                            }
                        }

                        if (hasBuckets == false)
                        {
                            //add the value to result
                            object value;
                            int index;

                            if (prop.Value is JValue)
                            {
                                //key value
                                value = ((JValue)prop.Value).Value;
                                index = _fieldNames.IndexOf(lastSeenKeyName);
                                lastValueWasKey = true;
                            }
                            else
                            {
                                //aggregated value
                                value = ((JValue)prop.Value.First.First).Value;
                                index = _fieldNames.IndexOf(prop.Name);
                                lastValueWasKey = false;
                            }

                            _currentRecord[index] = value;
                        }
                    }
                }

                ParseAggregations(child);
            }
        }

        private Type GetAggFieldType(JProperty agg)
        {
            var child = agg.Children().FirstOrDefault();
            if (child != null)
            {
                var bucketList = child["buckets"];
                if (bucketList != null)
                {
                    var bucket = bucketList.FirstOrDefault();
                    if (bucketList != null)
                    {
                        return GetFieldType(bucket["key"]);
                    }
                }
                else
                {
                    var value = child["value"];
                    if (value != null)
                    {
                        return GetFieldType(value);
                    }
                }
            }

            return typeof(object);
        }
    }
}