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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Runtime.CompilerServices;

    public class ElasticDbConnectionstring
    {
        private Dictionary<string, string> _connectionStringValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public ElasticDbConnectionstring()
        {
        }

        public ElasticDbConnectionstring(string connectionString)
        {
            ParseConnectionString(connectionString);
        }

        private void ParseConnectionString(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            _connectionStringValues = connectionString.Split(new[] { ';' })
                            .ToDictionary(
                            key => key.Split(new[] { '=' }).First(),
                            value => value.Split(new[] { '=' }).Last());

            ValidateParameter("Server");

            Server = _connectionStringValues["Server"];
        }

        private void ValidateParameter(string parameterName)
        {
            if (_connectionStringValues.ContainsKey(parameterName) == false || string.IsNullOrWhiteSpace(_connectionStringValues[parameterName]))
            {
                throw new ArgumentException($"The connectionstring does not contain a value for parameter {parameterName}");
            }
        }

        public string Server
        {
            get { return GetValue(); }
            set { SetValue(value: value); }
        }

        public Uri Uri
        {
            get
            {
                return new Uri($"http://{Server}");
            }
        }

        public string ConnectionString
        {
            get { return $"Server={Server};"; }
            set { ParseConnectionString(value); }
        }

        private void SetValue([CallerMemberName] string name = "", string value = "")
        {
            _connectionStringValues[name] = value;
        }

        private string GetValue([CallerMemberName] string name = "")
        {
            return _connectionStringValues[name];
        }
    }
}