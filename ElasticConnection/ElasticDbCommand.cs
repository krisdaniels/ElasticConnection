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
    using Newtonsoft.Json.Linq;
    using System;
    using System.Data;
    using System.Data.Common;
    using System.Linq;

    public class ElasticDbCommand : DbCommand
    {
        internal ElasticDbConnection ElasticDbConnection { get; private set;} 

        public ElasticDbCommand(ElasticDbConnection connection)
        {
            ElasticDbConnection = connection;
        }

        public ElasticDbCommand(string commandText, ElasticDbConnection connection) : this(connection)
        {
            CommandText = commandText;
        }

        public override string CommandText
        {
            get;
            set;
        }

        protected override DbConnection DbConnection
        {
            get { return ElasticDbConnection; }
            set
            {
                if (ElasticDbConnection.GetType().IsAssignableFrom(value.GetType()) == false)
                {
                    throw new ArgumentException($"Specified value of type {value.GetType()} does not implement ElasticDbConnection");
                }

                ElasticDbConnection = (ElasticDbConnection)value;
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            string query, verb, url;
            ParseCommantText(out query, out verb, out url);
            var json = ElasticDbConnection.ExecuteRequest(verb, url, query);
            var reader = new ElasticDbDataReader(json, ElasticDbConnection.ResultParserTypes);
            return reader;
        }

        private void ParseCommantText(out string query, out string verb, out string url)
        {
            var splitQuery = CommandText.Split(new[] { '\r', '\n' });
            var command = splitQuery.FirstOrDefault();
            query = string.Join("\n", splitQuery.Skip(1));
            verb = command.Split(new[] { ' ' }).FirstOrDefault();
            url = command.Split(new[] { ' ' }).LastOrDefault();
        }

        private int _commandTimeout;
        public override int CommandTimeout
        {
            get
            {
                return _commandTimeout;
            }

            set
            {
                _commandTimeout = value;
            }
        }
        
        public override int ExecuteNonQuery()
        {
            string query, verb, url;
            ParseCommantText(out query, out verb, out url);
            var json = ElasticDbConnection.ExecuteRequest(verb, url, query);

            JObject obj = JObject.Parse(json);
            if (obj["_shards"] != null)
            {
                return ((long)obj["_shards"]["failed"]) == 0 ? 1 : 0;
            }
            else if (obj["acknowledged"] != null)
            {
                return ((bool)obj["acknowledged"]) ? 1 : 0;
            }

            return 0;
        }

        public override CommandType CommandType
        {
            get
            {
                throw new NotSupportedException();
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                return false;
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                throw new NotSupportedException();
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        protected override DbTransaction DbTransaction
        {
            get
            {
                throw new NotSupportedException();
            }

            set
            {
                throw new NotSupportedException();
            }
        }

        public override void Cancel()
        {
            throw new NotSupportedException();
        }

        public override object ExecuteScalar()
        {
            throw new NotSupportedException();
        }

        public override void Prepare()
        {
            throw new NotSupportedException();
        }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotSupportedException();
        }
    }
}
