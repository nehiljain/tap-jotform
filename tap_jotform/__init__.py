#!/usr/bin/env python3
import os
import argparse
import json
import collections
import requests
import singer
import datetime
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
  'submissions': ['created_at'],
  'questions': ['qid'],
  'forms': ['id']
}

# need to move inside sync

URL = "https://hipaa-api.jotform.com/"

# need to be converted to clients
ENDPOINTS = {
  "forms": "/user/forms",
  "submissions": "/form/{form_id}/submissions"
}


class AuthException(Exception):
  pass

class NotFoundException(Exception):
  pass

def translate_state(state, catalog, form_ids):
  """
  state looks like:
    {
      "bookmarks": {
        "submissions": {
          "created_at": 1234412
        }
      }
    }
    This takes existing states and forms and makes sure that the new state has all the forms and bookmarks from existing state
  """
  nested_dict = lambda: collections.defaultdict(nested_dict)
  new_state = nested_dict()

  for stream in catalog['streams']:
    stream_name = stream['tap_stream_id']
    for form_id in form_ids:
      # TODO: check the merging of old state with new forms doesnt end up deleting the old state in lieu of completely fresh state
      if bookmarks.get_bookmark(state, form_id, stream_name):
        return state

      incremental_property = KEY_PROPERTIES[stream_name][0]
      if bookmarks.get_bookmark(state, stream_name, incremental_property):
        new_state['bookmarks'][form_id][stream_name][incremental_property] = bookarks.get_bookmark(state, stream_name, incremental_property)

  return new_state

def get_bookmark(state, form_id, stream_name, bookmark_key):
  form_stream_dict = bookmarks.get_bookmark(state, form_id, stream_name)
  if form_stream_dict:
      return form_stream_dict.get(bookmark_key)
  return None


def authed_get(source, url, query_params=None):
  query_params = query_params if query_params else {}
  with metrics.http_request_timer(source) as timer:

    resp = SESSION.get(url=url, params=query_params)
    resp.raise_for_status()

    timer.tags[metrics.Tag.http_status_code] = resp.status_code
    return resp

def authed_get_all_pages(source, url, query_params=None):
  query_params = query_params or {}
  offset, limit = 0, 20
  last_page = False
  while not last_page:
    resp = authed_get(source, url, query_params)
    resp.raise_for_status()
    yield resp

    result_set = resp.json().get('resultSet')
    try:
      if result_set.get('limit', limit) > result_set.get('count'):
        last_page = True
    except AttributeError as ae:
      last_page = True
    query_params['offset'] = query_params.get('offset', offset) + limit
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

def get_all_questions(schema, form_id, state, mdata):
  '''
  #todo: Needs to be a full table replication and not incremental. Change implementation accordingly
  https://hipaa-api.jotform.com/form/{form_id}/questions
  '''
  with metrics.record_counter('questions') as counter:
    for response in authed_get_all_pages(
      'questions',
      f'https://hipaa-api.jotform.com/form/{form_id}/questions',
      query_params=None
    ):
      questions = sorted([(q_key, q_value)
                          for q_key, q_value in
                          response.json().get('content').items()],
                          key=lambda x:int(x[0]))
      extraction_time = singer.utils.now()
      for qid, q_obj in questions:
        q_obj['form_id'] = form_id
        with singer.Transformer() as transformer:
          try:
            record = transformer.transform(q_obj, schema, metadata=metadata.to_map(mdata))
          except Exception as e:
            # LOGGER.exception(f'{form_id} and {q_id} data was not transformed due to error')
            continue
        singer.write_record('questions', record, time_extracted=extraction_time )

        counter.increment()

  return state


def get_all_submissions(schema, form_id, state, mdata):
  '''
  https://hipaa-api.jotform.com/form/{form_id}/submissions
  '''
  query_params = {}

  key_prop = KEY_PROPERTIES['submissions'][0]
  query_params['orderby'] = key_prop
  bookmark = get_bookmark(state, form_id, "submissions", key_prop)
  if bookmark:
    query_params["filter"] = json.dumps({f"{key_prop}:gt": bookmark})

  with metrics.record_counter('submissions') as counter:
    max_submission_creation_date = datetime.datetime.min
    for response in authed_get_all_pages(
      'submissions',
      f'https://hipaa-api.jotform.com/form/{form_id}/submissions',
      query_params=query_params,
    ):
      submissions = response.json()
      extraction_time = singer.utils.now()
      for submission in reversed(submissions.get('content')):
        with singer.Transformer() as transformer:
          record = transformer.transform(submission, schema, metadata=metadata.to_map(mdata))
        singer.write_record('submissions', record, time_extracted=extraction_time )
        max_submission_creation_date = max(datetime.datetime.strptime(submission[key_prop], '%Y-%m-%d %H:%M:%S'),
                                           max_submission_creation_date)
        singer.write_bookmark(state, form_id, 'submissions', {key_prop: max_submission_creation_date.strftime("%Y-%m-%d %H:%M:%S")})
        counter.increment()

  return state

def get_all_form_ids():
  for response in authed_get_all_pages(
      'forms',
      f'https://hipaa-api.jotform.com/user/forms',
      query_params=None,
    ):
    forms = response.json().get('content')
    for form in forms:
      yield form['id']

def get_all_forms(schema, form_id, state, mdata):

  key_prop = KEY_PROPERTIES['forms'][0]
  bookmark = get_bookmark(state, form_id, "forms", key_prop)
  with metrics.record_counter('forms') as counter:
    for response in authed_get_all_pages(
        'forms',
        f'https://hipaa-api.jotform.com/form/{form_id}',
        query_params=None,
      ):
      form = response.json().get('content')
      extraction_time = singer.utils.now()
      with singer.Transformer() as transformer:
        record = transformer.transform(form, schema, metadata=metadata.to_map(mdata))
      singer.write_record('forms', record, time_extracted=extraction_time )

      singer.write_bookmark(state, form_id, 'forms', {key_prop: form[key_prop]})
      counter.increment()

    return state

SYNC_FUNCTIONS = {
    'submissions': get_all_submissions,
    'questions': get_all_questions,
    'forms': get_all_forms
}

def do_sync(config, state, catalog):
  headers = {'APIKEY': config['api_key']}
  SESSION.headers.update(headers)

  selected_stream_ids = get_selected_streams(catalog)
  form_ids = list(get_all_form_ids())

  state = translate_state(state, catalog, form_ids)
  singer.write_state(state)

  for form_id in form_ids:
    LOGGER.info(f"Starting sync of submissions for form {form_id}")
    for stream in catalog['streams']:
      stream_id = stream['tap_stream_id']
      stream_schema = stream['schema']
      mdata = stream['metadata']

      if stream_id in selected_stream_ids:
        singer.write_schema(stream_id, stream_schema, stream['key_properties'])
        sync_function = SYNC_FUNCTIONS[stream_id]
        state = sync_function(stream_schema, form_id, state, mdata)
        singer.write_state(state)


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
