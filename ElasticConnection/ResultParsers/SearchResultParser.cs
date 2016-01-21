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
    using System.IO;

    public class SearchResultParser : ResultParser
    {
        private long _took, _total, _successful, _failed, _hits;
        private bool _timed_out;

        public SearchResultParser(string json) : base(json)
        {
            Length = 1;
            FieldNames = new[] { "Took", "TimedOut", "ShardsTotal", "ShardsSuccessful", "ShardsFailed", "TotalHits" };
            FieldTypes = new[] { typeof(long), typeof(bool), typeof(long), typeof(long), typeof(long), typeof(long) };
            ParseJson(json);
        }
        
        public override object GetValue(int ordinal)
        {
            switch (ordinal)
            {
                case 0: return _took;
                case 1: return _timed_out;
                case 2: return _total;
                case 3: return _successful;
                case 4: return _failed;
                case 5: return _hits;
            }

            return null;    
        }

        private void ParseJson(string json)
        {
            using (var stringReader = new StringReader(json))
            using (var jsonReader = new JsonTextReader(stringReader))
            {
                string currentProperty = string.Empty;
                bool inShards = false;
                while (jsonReader.Read())
                {
                    switch (jsonReader.TokenType)
                    {
                        case JsonToken.PropertyName:
                            currentProperty = jsonReader.Value.ToString();
                            break;

                        case JsonToken.StartObject:
                            if (string.CompareOrdinal(currentProperty, "_shards") == 0)
                                inShards = true;
                            break;

                        case JsonToken.EndObject:
                            if (inShards) inShards = false;
                            break;

                        case JsonToken.Integer:
                        case JsonToken.Boolean:
                            switch (currentProperty)
                            {
                                case "took": _took = (long)jsonReader.Value; break;
                                case "timed_out": _timed_out = (bool)jsonReader.Value; break;
                                case "total":
                                    if (inShards)
                                        _total = (long)jsonReader.Value;
                                    else
                                    {
                                        _hits = (long)jsonReader.Value;
                                        return;
                                    }
                                    break;

                                case "successful": _successful = (long)jsonReader.Value; break;
                                case "failed": _failed = (long)jsonReader.Value; break;
                            }

                            break;
                    }
                }
            }
        }
    }
}
