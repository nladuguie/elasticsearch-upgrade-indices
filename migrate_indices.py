#!/usr/bin/env python3

import argparse
import logging
import requests
import sys
import json

logging.captureWarnings(True)

# The Python script :
#   - Looks for Elasticsearch indices created with the parametered major version,
#   - Creates new indices,
#   - Reindex old indices in new indices,
#   - Deletes old indices.
# It takes the following parameters :
#   - Elasticsearch base URL with the port and without the trailing slash,
#   - The major version that indices should have been created with,
#   - The suffix applied to the new indices names.

def parse_arguments():
    parser = argparse.ArgumentParser(description='Elasticsearch ')
    parser.add_argument('-u', dest='elasticsearch_url', type=str, required=True)
    parser.add_argument('-v', dest='major_version_to_migrate', type=str, required=True)
    parser.add_argument('-s', dest='suffix_of_new_indices', type=str, required=False)

    return parser.parse_args()

def format_url_search_indices(args):
    url = "{elasticsearch_url}/_all/_settings/index.version*?format=json&pretty".format(
        elasticsearch_url=args.elasticsearch_url)
    return url

def format_url_get_indice_settings(args, indice_name):
    url = "{elasticsearch_url}/{indice_name}/_settings/index.number_*".format(
        elasticsearch_url=args.elasticsearch_url, indice_name=indice_name)
    return url

def format_url_get_indice_mappings(args, indice_name):
    url = "{elasticsearch_url}/{indice_name}/_mappings".format(
        elasticsearch_url=args.elasticsearch_url, indice_name=indice_name)
    return url

def format_url_create_indice(args, indice_name):
    url = "{elasticsearch_url}/{indice_name}".format(
        elasticsearch_url=args.elasticsearch_url, indice_name=indice_name)
    return url

def format_url_reindex_indice(args):
    url = "{elasticsearch_url}/_reindex".format(
        elasticsearch_url=args.elasticsearch_url)
    return url

def main():
    # Uncomment the following 2 lines to activate logging debug level
    # logging.basicConfig()
    # logging.getLogger().setLevel(logging.DEBUG)

    args = parse_arguments()

    # Set default suffix for new indices to "_new" if parameter is not provided
    new_indices_suffix = args.suffix_of_new_indices if args.suffix_of_new_indices is not None else '_new'

    # Search indices and their version in Elasticsearch
    url_elasticsearch_indices = format_url_search_indices(args)
    elasticsearch_indices_versions_response = requests.get(url_elasticsearch_indices, verify=False)

    if elasticsearch_indices_versions_response.status_code != 200:
        print("Can't retrieve Elasticsearch indices")
        sys.exit(2)

    elasticsearch_indices_versions = json.loads(elasticsearch_indices_versions_response.text)

    for indice_name, indice_settings in elasticsearch_indices_versions.iteritems():
        elasticsearch_indice_version_created = indice_settings["settings"]["index"]["version"]["created"]
        if elasticsearch_indice_version_created.startswith(args.major_version_to_migrate):
            migrate_indice(args, indice_name, new_indices_suffix)

    # # For each node running HiveServer2, we look for FINISHED and SUCCEEDED monitoring Job in Yarn Resource Manager
    # for hs2_node in hs2_nodes_list:
    #     current_hs2_node_hostname = hs2_node["hostname"]
    #     url_jobs_list =  format_url_yarn_jobs_list(args, current_hs2_node_hostname, beginning_period_timestamp)
    #     monitoring_jobs_list_result = requests.get(url_jobs_list, verify=False)
    #
    #     if monitoring_jobs_list_result.status_code != 200:
    #         print("Can't retrieve monitoring job list from Yarn Resource Manager")
    #         sys.exit(2)
    #
    #     monitoring_jobs_list_json = json.loads(monitoring_jobs_list_result.text)
    #     if monitoring_jobs_list_json["apps"] is None:
    #         hs2_nodes_not_running.append(current_hs2_node_hostname)
    #     else:
    #         monitoring_jobs_list = monitoring_jobs_list_json["apps"]["app"]
    #         if len(monitoring_jobs_list) == 0:
    #             hs2_nodes_not_running.append(current_hs2_node_hostname)

    # if len(hs2_nodes_not_running) > 0:
    #     print('HiveServer2 not running : ' + ','.join(hs2_nodes_not_running))
    # else:
    #     print("OK")
    sys.exit(0)

def migrate_indice(args, indice_name, new_indices_suffix):
    new_indice_name = indice_name + new_indices_suffix
    # Retrieve indice settings and mappings to apply them to new indice
    url_elasticsearch_indice_settings = format_url_get_indice_settings(args)
    elasticsearch_old_indice_settings_response = requests.get(url_elasticsearch_indice_settings, verify=False)
    if elasticsearch_old_indice_settings_response.status_code != 200:
        print("Can't retrieve Elasticsearch indice settings")
        sys.exit(2)
    elasticsearch_old_indice_settings = json.loads(elasticsearch_old_indice_settings_response.text)

    url_elasticsearch_indice_mappings = format_url_get_indice_mappings(args)
    elasticsearch_old_indice_mappings_response = requests.get(url_elasticsearch_indice_mappings, verify=False)
    if elasticsearch_old_indice_mappings_response.status_code != 200:
        print("Can't retrieve Elasticsearch indice mappings")
        sys.exit(2)
    elasticsearch_old_indice_mappings = json.loads(elasticsearch_old_indice_mappings_response.text)

    # Build indice creation request body with settings and mappings from old indice
    elasticsearch_create_indice_body = {
        "settings" : {
            "index" : {
                "number_of_shards" : elasticsearch_old_indice_settings[indice_name]["settings"]["index"]["number_of_shards"],
                "number_of_replicas" : elasticsearch_old_indice_settings[indice_name]["settings"]["index"]["number_of_replicas"]
            }
        },
        "mappings": elasticsearch_old_indice_mappings["elasticsearch_indice_name"]["mappings"]
    }

    # Create new indice
    url_elasticsearch_create_indice = format_url_create_indice(args, new_indice_name)
    elasticsearch_create_indice_response = requests.put(url_elasticsearch_create_indice, verify=False, data = elasticsearch_create_indice_body)
    if elasticsearch_create_indice_response.status_code != 200:
        print("Can't create Elasticsearch indice")
        sys.exit(2)

    # Build reindex request body
    elasticsearch_reindex_body = {
        "source": {
            "index": indice_name
        },
        "dest": {
            "index": new_indice_name
        }
    }

    # Reindex old indice to new indice
    url_elasticsearch_reindex_indice = format_url_reindex_indice(args)
    elasticsearch_reindex_indice_response = requests.post(url_elasticsearch_reindex_indice, verify=False, data = elasticsearch_reindex_body)
    if elasticsearch_reindex_indice_response.status_code != 200:
        print("Can't reindex Elasticsearch indice")
        sys.exit(2)

    # Remove old indice and create alias of old indice name to new indice


    elasticsearch_reindex_body = {
        "source": {
            "index": indice_name
        },
        "dest": {
            "index": new_indice_name
        }
    }

if __name__ == '__main__':
    main()
