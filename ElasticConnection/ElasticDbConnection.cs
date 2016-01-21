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
    using ElasticObjects;
    using Newtonsoft.Json;
    using ResultParsers;
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.IO;
    using System.Net;
    using System.Text;

    public class ElasticDbConnection : DbConnection, ICloneable
    {
        private HttpWebRequest _request;
        
        public ElasticDbConnection(string connectionString) 
            : this(new ElasticDbConnectionstring(connectionString))
        {
        }

        public ElasticDbConnection(ElasticDbConnectionstring connectionstring)
            :this(connectionstring, new[] { typeof(HitsParser) })
        {
        }

        public ElasticDbConnection(string connectionString, IEnumerable<Type> resultParsers)
            :this(new ElasticDbConnectionstring(connectionString), resultParsers)
        {
        }

        public ElasticDbConnection(ElasticDbConnectionstring connectionstring, IEnumerable<Type> resultParsers)
        {
            foreach (var parser in resultParsers)
            {
                if (typeof(IResultParser).IsAssignableFrom(parser) == false)
                {
                    throw new ArgumentException($"Resultparser {parser.GetType().FullName} does not implement IResultParser");
                }
            }

            if (connectionstring == null)
            {
                throw new ArgumentNullException(nameof(connectionstring));
            }

            ElasticDbConnectionstring = connectionstring;
            ResultParserTypes = resultParsers;
        }

        public ElasticDbConnectionstring ElasticDbConnectionstring { get; private set; }
        public IEnumerable<Type> ResultParserTypes { get; private set; }

        public override string ConnectionString
        {
            get
            {
                return ElasticDbConnectionstring.ConnectionString;
            }

            set
            {
                ElasticDbConnectionstring = new ElasticDbConnectionstring(value);
            }
        }

        private ConnectionState _state;
        public override ConnectionState State
        {
            get { return _state; }
        }

        public override void Close()
        {
            if (_request != null)
            {
                _request.Abort();
                _request = null;
            }

            ServerInfo = null;
            _state = ConnectionState.Closed;
        }

        public override void Open()
        {
            _state = ConnectionState.Connecting;

            var json = ExecuteRequest("GET", "", null);
            ServerInfo = JsonConvert.DeserializeObject<ElasticsearchInfo>(json);

            _state = ConnectionState.Open;
        }

        public override string Database
        {
            get
            {
                return ElasticDbConnectionstring.Server;
            }
        }

        public override string DataSource
        {
            get
            {
                return ElasticDbConnectionstring.Server;
            }
        }

        public ElasticsearchInfo ServerInfo { get; private set; }

        public override string ServerVersion
        {
            get
            {
                return ServerInfo.Version.Number;
            }
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException("Database names are not supported, the index is specified in the query");
        }

        public object Clone()
        {
            return new ElasticDbConnection(ElasticDbConnectionstring);
        }

        private Dictionary<int, HttpWebRequest> _requests = new Dictionary<int, HttpWebRequest>();
        public StreamWriter BeginBulkLoad()
        {
            var request = GetRequest("POST", "/_bulk");
            var stream = request.GetRequestStream();
            var writer = new StreamWriter(stream);
            _requests.Add(writer.GetHashCode(), request);
            return writer;
        }

        public BulkResult EndBulkLoad(StreamWriter writer, bool returnResults = true)
        {
            var request = _requests[writer.GetHashCode()];
            writer.Flush();
            writer.BaseStream.Flush();
            writer.BaseStream.Dispose();
            writer.Dispose();

            try
            {
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    if (returnResults)
                    {
                        string json = string.Empty;
                        using (var responseStream = response.GetResponseStream())
                        using (var reader = new StreamReader(responseStream))
                        {
                            json = reader.ReadToEnd();
                        }

                        return BulkResult.Parse(json);
                    }
                    else
                    {
                        return null;
                    }
                }
            }
            catch (WebException webEx)
            {
                string result = string.Empty;
                using (var exStream = webEx.Response.GetResponseStream())
                using (var reader = new StreamReader(exStream))
                {
                    result = reader.ReadToEnd();
                }

                throw new WebException(result, webEx);
            }
        }

        Stopwatch w = new Stopwatch();
        internal string ExecuteRequest(string verb, string relativeUrl, string data)
        {
            w.Restart();
            var request = GetRequest(verb, relativeUrl);
            if (request.Method != "GET")
            {
                using (var requestStream = request.GetRequestStream())
                {
                    var bytes = Encoding.UTF8.GetBytes(data);
                    requestStream.Write(bytes, 0, bytes.Length);
                }
            }

            try
            {
                string json = null;
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {

                    if (response.StatusCode == HttpStatusCode.OK)
                    {
                        using (var responseStream = response.GetResponseStream())
                        using (var reader = new StreamReader(responseStream))
                        {
                            json = reader.ReadToEnd();
                        }
                    }
                }
                w.Stop();
                Trace.WriteLine($"Executed http request in {w.ElapsedMilliseconds} ms");

                return json;
            }
            catch (WebException webEx)
            {
                string result = string.Empty;
                using (var exStream = webEx.Response.GetResponseStream())
                using (var reader = new StreamReader(exStream))
                {
                    result = reader.ReadToEnd();
                }

                throw new WebException(result, webEx);
            }

        }

        internal HttpWebRequest GetRequest(string verb, string relativeUrl)
        {
            var request = WebRequest.CreateHttp(new Uri(ElasticDbConnectionstring.Uri, relativeUrl));
            request.Method = verb;
            request.KeepAlive = false;
            return request;
        }

        protected override DbCommand CreateDbCommand()
        {
            return new ElasticDbCommand(this);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotSupportedException("Transactions are not supported by Elasticsearch");
        }
    }
}
