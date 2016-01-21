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
    using System;

    public class CustomParser : ResultParser
    {
        public CustomParser(string json) : base(json)
        {
            // The json string containt the data that is received from Elasticsearch
        }

        public override object GetValue(int ordinal)
        {
            // current row index is available in base._position, or override MoveNext to keep your own counter

            // return the value for row _position and fieldindex ordrinal
            throw new NotImplementedException();
        }
    }
}
