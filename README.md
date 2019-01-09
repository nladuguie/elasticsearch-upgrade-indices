# elasticsearch-upgrade-indices

Elasticsearch doesn't allow a server to boot when too old indices are detected in the data folder.
"Old indices" are created in an older major version than the previous one.
So if you want to keep your old indices, you have to reindex them, in an appropriate Elasticsearch version.

## Compatible versions
This script has been tested and used to migrate Elasticsearch 2.3 indices, with Elasticsearch 5.4, to be used in Elasticsearch 6.3. 

## Usage
This script doesn't work with Elasticsearch secured cluster (no need to authenticate).
