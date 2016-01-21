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
    using System;
    using System.Collections.Generic;
    using System.IO;

    public class HitsParser : ResultParser, IDisposable
    {
        private object[] currentHit;
        private List<string> _fieldNames = new List<string>();
        private List<Type> _fieldTypes = new List<Type>();

        private StringReader stringReader;
        private JsonTextReader jsonReader;

        public HitsParser(string json) : base(json)
        {
            stringReader = new StringReader(json);
            jsonReader = new JsonTextReader(stringReader);

            var maxScore = GetMaxScore(jsonReader); // score = 0 => no results found

            if (maxScore == 0)
            {
                // Provide dummy fields
                FieldNames = new[] { "dummy" };
                FieldTypes = new[] { typeof(string) };
            }
            else
            {
                hasResults = true;
                // advance to startArray
                while (jsonReader.Read() && jsonReader.TokenType != JsonToken.StartArray) ;

                ReadInitialHit(jsonReader);

                FieldNames = _fieldNames.ToArray();
                FieldTypes = _fieldTypes.ToArray();
            }
        }

        private bool inSource = false;
        private void ReadInitialHit(JsonTextReader jsonReader)
        {
            // advance to StartObject
            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.StartObject) ;

            List<object> initialValues = new List<object>();

            // read initial hit until EndObject
            inSource = false;
            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
            {
                if (jsonReader.TokenType == JsonToken.PropertyName)
                {
                    if (!inSource && string.CompareOrdinal((string)jsonReader.Value, "_source") == 0)
                    {
                        jsonReader.Read();
                        jsonReader.Read();
                        inSource = true;
                    }

                    _fieldNames.Add((string)jsonReader.Value);

                    jsonReader.Read(); // read value
                    _fieldTypes.Add(jsonReader.ValueType);
                    initialValues.Add(jsonReader.Value);
                }
            }

            currentHit = initialValues.ToArray();
        }

        private bool result = true;
        private int _currentField = 0;
        private bool ReadNextHit(JsonTextReader jsonReader)
        {
            // advance to StartObject
            while ((result = jsonReader.Read()) && (jsonReader.TokenType != JsonToken.StartObject || jsonReader.TokenType == JsonToken.EndArray)) ;
            if (!result || jsonReader.TokenType == JsonToken.EndArray) return result;

            _currentField = 0;

            // read initial hit until EndObject
            inSource = false;
            while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndObject)
            {
                if (jsonReader.TokenType == JsonToken.PropertyName)
                {
                    if (!inSource && string.CompareOrdinal((string)jsonReader.Value, "_source") == 0)
                    {
                        jsonReader.Read();
                        jsonReader.Read();
                        inSource = true;
                    }

                    jsonReader.Read(); // read value
                    currentHit[_currentField++] = jsonReader.Value;
                }
            }

            return result;
        }

        private bool initialHitWasRead = false;
        private bool hasResults = false;
        public override bool MoveNext()
        {
            if (initialHitWasRead)
                return ReadNextHit(jsonReader);

            if (!hasResults) return hasResults;

            initialHitWasRead = true;
            return true;
        }

        public override object GetValue(int ordinal)
        {
            return currentHit[ordinal];
        }

        private static double GetMaxScore(JsonTextReader jsonReader)
        {
            string currentProperty = string.Empty;
            int readCount = 0;
            while (jsonReader.Read() && readCount < 20)
            {
                if (jsonReader.TokenType == JsonToken.PropertyName && string.CompareOrdinal(jsonReader.Value.ToString(), "hits") == 0)
                {
                    jsonReader.Read(); // StartObject
                    jsonReader.Read(); // PropertyName = total
                    jsonReader.Read(); // total Value
                    jsonReader.Read(); // PropertyName = maxScore
                    jsonReader.Read(); // maxScore value
                    return (double)jsonReader.Value;
                }

                readCount++;
            }

            return 0;
        }
        
        public void Dispose()
        {
            if (jsonReader != null)
            {
                jsonReader.Close();
                jsonReader = null;
            }

            if (stringReader != null)
            {
                stringReader.Dispose();
                stringReader = null;
            }
        }
    }
}
