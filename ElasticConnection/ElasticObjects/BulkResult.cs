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

namespace ElasticConnection.ElasticObjects
{
    using Newtonsoft.Json.Linq;
    using System.Collections.Generic;
    using System.Linq;

    public enum BulkResultType
    {
        Unknown,
        Create,
        Delete,
        Update
    }

    public class BulkResult
    {
        private JObject _data;

        private BulkResult()
        {
        }

        public int Took { get { return _data["took"].Value<int>(); } }
        public bool Errors { get { return _data["errors"].Value<bool>(); } }
        public IEnumerable<BulkResultItem> Items { get { return _data["items"].Select(i => BulkResultItem.Create((JObject)i)); } }

        public static BulkResult Parse(string json)
        {
            var result = new BulkResult();
            result._data = JObject.Parse(json);
            return result;
        }
    }
   
    public class BulkResultItem
    {
        private BulkResultItem()
        {
        }

        private JToken _data;

        internal static BulkResultItem Create(JObject obj)
        {
            var item = new BulkResultItem();
            var prop = obj.Properties().First();
            item._data = prop.Children().First();

            switch (prop.Name)
            {
                case "create": item.ResultType = BulkResultType.Create; break;
                case "update": item.ResultType = BulkResultType.Update; break;
                case "delete": item.ResultType = BulkResultType.Delete; break;
                default: item.ResultType = BulkResultType.Unknown; break;
            };

            return item;
        }

        public BulkResultType ResultType { get; private set; }
        public string Index { get { return _data["_index"].Value<string>(); } }
        public string DocType { get { return _data["_type"].Value<string>(); } }
        public string Id { get { return _data["_id"].Value<string>(); } }
        public int Version { get { return _data["_version"].Value<int>(); } }
        public int ShardsTotal { get { return _data["_shards"]["total"].Value<int>(); } }
        public int ShardsSuccessful { get { return _data["_shards"]["successful"].Value<int>(); } }
        public int ShardsFailed { get { return _data["_shards"]["failed"].Value<int>(); } }
        public int Status { get { return _data["status"].Value<int>(); } }
        public bool? Found { get { return _data["found"]?.Value<bool?>(); } }
    }
}
