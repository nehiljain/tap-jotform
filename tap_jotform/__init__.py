#!/usr/bin/env python3
import os
import argparse
import json
import collections
import requests
import singer
from singer import utils, metadata
import singer.bookmarks as bookmarks
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
import singer.metrics as metrics

from .metadata_utils import write_metadata, populate_metadata, get_selected_streams

REQUIRED_CONFIG_KEYS = ["api_key", "start_date", "is_hipaa_safe_mode_on"]
PER_PAGE = 100

SESSION = requests.Session()
LOGGER = singer.get_logger()

KEY_PROPERTIES = {
  'submission': ['id']
}

# need to move inside sync

URL = "https://hipaa-api.jotform.com/"

# need to be converted to clients
ENDPOINTS = {
  "forms": "user/forms",
  "submissions": "/form/{form_id}/submissions"
}


class AuthException(Exception):
  pass

class NotFoundException(Exception):
  pass

def translate_state(state, catalog, form_id):
  """
  state looks like:
    {
      "bookmarks": {
        "submissions": {
          "id": 1234412
        }
      }
    }
  """
  pass

def get_bookmark(state, form_id, stream_name, bookmark_key):
  pass


def authed_get(source, url, query_params=None):
  query_params = query_params if query_params else {}
  with metrics.http_request_timer(source) as timer:

    resp = SESSION.get(url=url, params=query_params)
    resp.raise_for_status()

    timer.tags[metrics.Tag.http_status_code] = resp.status_code
    return resp

def authed_get_all_pages(source, url, query_params):
  offset, limit = 0, 20
  while True:
    resp = authed_get(source, url, query_params)
    resp.raise_for_status()
    yield resp
    query_params['offset'] = query_params['offset'] + limit
    query_params['limit'] = limit



def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema, KEY_PROPERTIES)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def get_all_submissions(schema, form_id, state, mdata):
  '''
  https://hipaa-api.jotform.com/form/{form_id}/submissions
  '''
  query_params = {}
  bookmark = get_bookmark(state, form_id, "submissions", "id")
  if bookmark:
    query_params["id:gt"] = bookmark

  with metrics.record_counter('submissions') as counter:
    for response in authed_get_all_pages(
      'submissions',
      f'https://hipaa-api.jotform.com/form/{form_id}/submissions',
      query_params=query_params,
    ):
      submissions = response.json()
      extraction_time = singer.utils.now()
      for submission in submissions.get('content'):
        # Transform the event
        with singer.Transformer() as transformer:
                    record = transformer.transform(submission, schema, metadata=metadata.to_map(mdata))
        singer.write_record('submissions', record, time_extracted=extraction_time )
        singer.write_bookmark(state, form_id, 'submissions', {'id': submissions['id']})
        counter.increment()

  return state

def do_sync(config, state, catalog):
  headers = {'APIKEY': config['api_key']}
  SESSION.headers.update(headers)

  # get selected streams, make sure stream dependencies are met
  selected_stream_ids = get_selected_streams(catalog)
  print(selected_stream_ids)
  import ipdb; ipdb.set_trace()

  state = translate_state(state, catalog)
  singer.write_state(state)

  # get all the users
  # get all the forms from api

  for form_id in form_ids:
    logger.info(F"Starting sync of submissions for form {form_id}")
    for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

  # get all the submissions for all the forms
  # get all the questions for all the forms



@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
