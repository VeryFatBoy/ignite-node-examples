/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const Util = require('util');
const IgniteClient = require('apache-ignite-client');
const ObjectType = IgniteClient.ObjectType;
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const CacheConfiguration = IgniteClient.CacheConfiguration;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;

const ENDPOINT = '127.0.0.1:10800';

const COUNTRY_CACHE_NAME = 'CountryCache';
const CITY_CACHE_NAME = 'CityCache';

class ImportedCluster2 {
    async start() {
        const igniteClient = new IgniteClient(this.onStateChanged.bind(this));
        try {
            await igniteClient.connect(new IgniteClientConfiguration(ENDPOINT));

            const countryCache = igniteClient.getCache(COUNTRY_CACHE_NAME);
            const cityCache = igniteClient.getCache(CITY_CACHE_NAME);

            await this.getTopCitiesInThreeCountries(cityCache);
        }
        catch (err) {
            console.log('ERROR: ' + err.message);
        }
        finally {
            igniteClient.disconnect();
        }
    }

    async getTopCitiesInThreeCountries(countryCache) {
        const query = new SqlFieldsQuery(
            `SELECT country.name, city.name, MAX(city.population)
             AS max_pop FROM country
             JOIN city ON city.countrycode = country.code
             WHERE country.code IN ('USA','RUS','CHN')
             GROUP BY country.name, city.name
             ORDER BY max_pop DESC LIMIT 3`);

        const cursor = await countryCache.query(query);

        console.log("3 Most Populated Cities in US, RUS and CHN:");

        for (let row of await cursor.getAll()) {
            console.log("    " + row[2] + " people live in " + row[1] + ", " + row[0]);
        }
    }

    onStateChanged(state, reason) {
        if (state === IgniteClient.STATE.CONNECTED) {
            console.log('Client is started');
        }
        else if (state === IgniteClient.STATE.DISCONNECTED) {
            console.log('Client is stopped');
            if (reason) {
                console.log(reason);
            }
        }
    }
}

const importedCluster2 = new ImportedCluster2();
importedCluster2.start();
