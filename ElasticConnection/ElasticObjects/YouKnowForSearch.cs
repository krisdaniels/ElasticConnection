﻿/**
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
    using Newtonsoft.Json;

    public class ElasticsearchInfo
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("cluster_name")]
        public string ClusterName { get; set; }

        [JsonProperty("version")]
        public ElasticsearchVersion Version { get; set; }

        [JsonProperty("tagline")]
        public string Tagline { get; set; }
    }

    public class ElasticsearchVersion
    {
        [JsonProperty("number")]
        public string Number { get; set; }

        [JsonProperty("build_hash")]
        public string BuildHash { get; set; }

        [JsonProperty("build_timestamp")]
        public string BuildTimestamp { get; set; }

        [JsonProperty("build_snapshot")]
        public bool BuildSnapshot { get; set; }

        [JsonProperty("lucene_version")]
        public string LuceneVersion { get; set; }
    }
}
