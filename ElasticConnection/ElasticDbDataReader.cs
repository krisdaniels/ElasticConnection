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

namespace ElasticConnection
{
    using ElasticConnection.ResultParsers;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;

    public class ElasticDbDataReader : DbDataReader
    {
        private int _currentParserIndex = 0;
        private IResultParser[] _resultParserInstances;
        private IEnumerable<Type> _resultParserTypes;

        private IResultParser CurrentParser
        {
            get
            {
                if (_currentParserIndex < _resultParserInstances.Length)
                {
                    return _resultParserInstances[_currentParserIndex];
                }
                else
                {
                    return null;
                }
            }
        }

        public ElasticDbDataReader(string json, IEnumerable<Type> resultParserTypes)
        {
            _resultParserTypes = resultParserTypes;
            //var obj = JObject.Parse(jsonData);
            SetupParsers(json);
        }
        
        private void SetupParsers(string json)
        {
            List<IResultParser> parsers = new List<IResultParser>();
            foreach (var parser in _resultParserTypes)
            {
                parsers.Add((IResultParser)Activator.CreateInstance(parser, json));
            }
           
            _resultParserInstances = parsers.ToArray();
            _currentParserIndex = 0;
        }

        public override int FieldCount
        {
            get
            {
                return CurrentParser?.FieldCount ?? 0;
            }
        }

        public override bool IsClosed
        {
            get
            {
                return false;
            }
        }

        public override string GetName(int ordinal)
        {
            return CurrentParser.FieldNames[ordinal];
        }

        public override bool Read()
        {
            return CurrentParser.MoveNext();
        }

        public override object GetValue(int ordinal)
        {
            return CurrentParser.GetValue(ordinal);
        }

        public override object this[int ordinal]
        {
            get
            {
                return GetValue(ordinal);
            }
        }

        public override bool NextResult()
        {
            _currentParserIndex++;
            return _currentParserIndex < _resultParserInstances.Length;
        }

        public override Type GetFieldType(int ordinal)
        {
            return CurrentParser.FieldTypes[ordinal];
        }

        public override object this[string name]
        {
            get
            {
                return GetOrdinal(name);
            }
        }

        public override int Depth
        {
            get
            {
                return 0;
            }
        }

        public override bool HasRows
        {
            get
            {
                return FieldCount > 0;
            }
        }

        public override int RecordsAffected
        {
            get
            {
                return 0;
            }
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool)GetValue(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)GetValue(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override char GetChar(int ordinal)
        {
            return (char)GetValue(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return GetFieldType(ordinal).Name;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double)GetValue(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return CurrentParser;
        }

        public override float GetFloat(int ordinal)
        {
            return (float)GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return (Guid)GetValue(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return (short)GetValue(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return (int)GetValue(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return (long)GetValue(ordinal);
        }


        public override int GetOrdinal(string name)
        {
            return Array.IndexOf(CurrentParser.FieldNames, name);
        }

        public override string GetString(int ordinal)
        {
            return (string)GetValue(ordinal);
        }

        public override int GetValues(object[] values)
        {
            values = Enumerable.Range(0, CurrentParser.FieldCount).Select(i => GetValue(i)).ToArray();
            return values.Length;
        }

        public override bool IsDBNull(int ordinal)
        {
            return GetValue(ordinal) == null;
        }
    }
}
