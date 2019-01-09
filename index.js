#!/usr/bin/env node

/**
 * Module dependencies.
 */

const program = require('commander');
const request = require('request');
const { Confirm } = require('enquirer');

program
    .option('-u, --elasticsearch-url <elasticsearchUrl>', 'Elasticsearch URL ending with port without trailing slash')
    .option('-m, --major-version-to-migrate <majorVersionToMigrate>', 'major_version_to_migrate')
    .option('-s, --suffix-of-new-indices [suffixOfNewIndices]', 'suffix_of_new_indices', '_new')
    .parse(process.argv);

request.get(`${program.elasticsearchUrl}/_all/_settings/index.version*?format=json&pretty`, (error, response, body) => {
    if (error) {
        return console.error(`Can't retrieve Elasticsearch indices : ${error}`);
    }
    const elasticsearchIndices = JSON.parse(body);

    const elasticsearchOldIndices = [];

    for (const indiceName in elasticsearchIndices) {
        const elasticsearchIndiceVersionCreated = elasticsearchIndices[indiceName].settings.index.version.created;
        if (elasticsearchIndiceVersionCreated.startsWith(program.majorVersionToMigrate)) {
            elasticsearchOldIndices.push(indiceName);
        }
    }

    console.log(`Old indices found : \r\n${elasticsearchOldIndices.join('\r\n')}`);

    const confirmationPrompt = new Confirm({
        name: 'really',
        message: 'Do you want to migrate those indices ?'
    });

    confirmationPrompt.isTrue = input => input === 'y';
    confirmationPrompt.isFalse = input => input === 'n';

    confirmationPrompt.run()
        .then((answer) => {
            if (answer) {
                for (const indiceName of elasticsearchOldIndices) {
                    migrateIndice(indiceName)
                        .then(result => {
                            console.log(`Successfully migrated index ${indiceName}`);
                        })
                        .catch(error => {
                            console.error(`Can't migrate index ${indiceName}`);
                        });
                }
            } else {
                console.warn('Indices migration cancelled by user.')
            }
        })
        .catch(() => console.error('Error while prompting user\'s confirmation'));
});

function migrateIndice(indiceName) {
    const newIndiceName = `${indiceName}${program.suffixOfNewIndices}`;
    return new Promise((resolve, reject) => {
        Promise.all([retrieveIndiceSettings(indiceName), retrieveIndiceMappings(indiceName)])
            .then(values => {
                return createIndex(newIndiceName, values[0][indiceName].settings, values[1][indiceName].mappings);
            })
            .then(result => resolve(true))
            .catch(error => {
                reject(error);
            });

    });
}

function retrieveIndiceSettings(indiceName) {
    return new Promise((resolve, reject) => {
        request.get(`${program.elasticsearchUrl}/${indiceName}/_settings/index.number_*`, (error, response, body) => {
            if (error || response.statusCode != 200) {
                console.error(`Can't retrieve Elasticsearch ${indiceName} indice settings`);
                reject(false);
            } else {
                console.debug(`Successfully retrieved Elasticsearch ${indiceName} indice settings`);
                const indiceSettings = JSON.parse(body);
                resolve(indiceSettings);
            }
        });
    });
}

function retrieveIndiceMappings(indiceName) {
    return new Promise((resolve, reject) => {
        request.get(`${program.elasticsearchUrl}/${indiceName}/_mappings`, (error, response, body) => {
            if (error || response.statusCode != 200) {
                console.error(`Can't retrieve Elasticsearch ${indiceName} indice mappings`);
                reject(false);
            } else {
                console.debug(`Successfully retrieved Elasticsearch ${indiceName} indice mappings`);
                const indiceMappings = JSON.parse(body);
                resolve(indiceMappings);
            }
        });
    });
}

function createIndex(indiceName, settings, mappings) {
    return new Promise((resolve, reject) => {
        const createIndiceBody = {
            settings : {
                index : {
                    number_of_shards : settings.index.number_of_shards,
                    number_of_replicas : settings.index.number_of_replicas
                }
            },
            mappings: mappings
        };

        const createIndexRequestOptions = {
            method: 'PUT',
            url: `${program.elasticsearchUrl}/${indiceName}`,
            body: JSON.stringify(createIndiceBody)
        };

        request.put(createIndexRequestOptions, (error, response, body) => {
            if (error || response.statusCode != 200) {
                console.error(`Can't create Elasticsearch ${indiceName} indice`);
                reject(false);
            } else {
                console.log(`Successfully created Elasticsearch ${indiceName} indice`);
                resolve(true);
            }
        });
    });
}

function reindexIndice() {
    return new Promise((resolve, reject) => {
        const reindexIndiceBody = {
            settings : {
                index : {
                    number_of_shards : settings.index.number_of_shards,
                    number_of_replicas : settings.index.number_of_replicas
                }
            },
            mappings: mappings
        };

        const reindexIndiceRequestOptions = {
            method: 'PUT',
            url: `${program.elasticsearchUrl}/_reindex`,
            body: JSON.stringify(reindexIndiceBody)
        };

        request.put(reindexIndiceRequestOptions, (error, response, body) => {
            if (error || response.statusCode != 200) {
                console.error(`Can't reindex Elasticsearch ${indiceName} indice`);
                reject(false);
            } else {
                console.log(`Successfully reindex Elasticsearch ${indiceName} indice`);
                resolve(true);
            }
        });
    });
}