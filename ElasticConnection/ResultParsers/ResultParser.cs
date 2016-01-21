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
    using Newtonsoft.Json.Linq;
    using System;

    public abstract class ResultParser : IResultParser
    {
        protected int _position = -1;

        public ResultParser(string json)
        {
            this.FieldNames = new string[0];
            this.FieldTypes = new Type[0];
        }

        public virtual object Current
        {
            get
            {
                throw new NotSupportedException("Use GetValue instead");
            }
        }

        public virtual int FieldCount
        {
            get
            {
                return FieldNames.Length;
            }
        }

        public virtual string[] FieldNames
        {
            get;
            protected set;
        }

        public virtual Type[] FieldTypes
        {
            get;
            protected set;
        }

        public virtual object GetValue(string name)
        {
            return GetValue(Array.IndexOf(FieldNames, name));
        }

        public long Length { get; protected set; }
        public abstract object GetValue(int ordinal);

        public virtual bool MoveNext()
        {
            _position++;
            return _position < Length;
        }


        public virtual void Reset()
        {
            _position = -1;
        }

        protected virtual Type GetFieldType(JToken token)
        {
            return GetFieldType(token.Type);
        }

        protected virtual Type GetFieldType(JProperty property)
        {
            var jsonType = property.Value.Type;
            return GetFieldType(jsonType);
        }

        protected virtual Type GetFieldType(JValue value)
        {
            var jsonType = value.Type;
            return GetFieldType(jsonType);
        }

        protected virtual Type GetFieldType(JTokenType jsonType)
        { 
            switch (jsonType)
            {
                case JTokenType.Array:
                    return typeof(Array);

                case JTokenType.Raw:
                case JTokenType.String:
                    return typeof(string);

                case JTokenType.Integer:
                    return typeof(long);

                case JTokenType.Float:
                    return typeof(double);

                case JTokenType.Boolean:
                    return typeof(bool);

                case JTokenType.Null:
                    return typeof(DBNull);

                case JTokenType.Date:
                    return typeof(DateTime);

                case JTokenType.Bytes:
                    return typeof(byte[]);

                case JTokenType.Guid:
                    return typeof(Guid);

                case JTokenType.Uri:
                    return typeof(Uri);

                case JTokenType.TimeSpan:
                    return typeof(TimeSpan);

                case JTokenType.Comment:
                case JTokenType.Constructor:
                case JTokenType.Property:
                    throw new NotSupportedException();

                default:
                case JTokenType.Undefined:
                case JTokenType.None:
                case JTokenType.Object:
                    return typeof(object);
            }
        }
    }
}
