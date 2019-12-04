import datetime
import json
import os
import re

import requests
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util import parse_url
from urllib3.util.retry import Retry

from moai.utils import check_type, ProgressBar

DIRECTUS_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DIRECTUS_API_PATTERN = '(directus://)(.*)'
RECORDS_PER_REQUEST = 150


class Directus:
    def __init__(self, dburi, config=None, email='', pwd=''):
        match = re.match(DIRECTUS_API_PATTERN, dburi)
        if not match:
            raise Exception(f'{dburi}: Invalid Directus API URL given, should be of pattern {DIRECTUS_API_PATTERN}')

        self.api_url = match.group(2)
        self.session = requests.Session()
        retry = Retry(
            backoff_factor=0.3,
            status_forcelist=(500, 503, 413, 429),
            method_whitelist=('POST', 'PATCH', 'OPTIONS', 'PUT', 'GET', 'TRACE', 'HEAD', 'DELETE')
        )
        self.session.mount(f'{parse_url(self.api_url).scheme}://', HTTPAdapter(max_retries=retry))
        self._reset_cache()
        self._is_first_flush = True

        self.staticToken = False
        if config and 'directus_auth_token' in config:
            self.staticToken = True
            self.session.headers.update({'Authorization': f'Bearer {config["directus_auth_token"]}'})

        self._refresh_token(config['directus_auth_email'] if config and 'directus_auth_email' in config else email,
                            config['directus_auth_pwd'] if config and 'directus_auth_pwd' in config else pwd)

    def _refresh_token(self, email='', pwd=''):
        if self.staticToken:
            return

        if not self.session.headers.get('Authorization'):
            auth_route = 'authenticate'
            auth_data = {'email': os.getenv('DIRECTUS_AUTH_EMAIL', email),
                         'password': os.getenv('DIRECTUS_AUTH_PWD', pwd)}
        else:
            auth_route = 'refresh'
            auth_data = {'token': self.session.headers.get('Authorization').split()[1]}

        auth_response = self.session.post(f'{self.api_url}/auth/{auth_route}', data=auth_data)
        if auth_response.status_code != 200:
            raise HTTPError(auth_response.json()['error']['message'], auth_response)

        self.session.headers.update({'Authorization': f'Bearer {auth_response.json()["data"]["token"]}'})

    def flush(self):
        inserted_records = []
        for record_id, record in list(self._cache['records'].items()):
            record['id'] = record_id
            record['modified'] = record['modified'].isoformat()
            inserted_records.append(record)

        inserted_sets = []
        for set_id, set in list(self._cache['sets'].items()):
            set['id'] = set_id
            set['name'] = set_id
            set['records'] = [{'record_id': record} for record in inserted_records]
            inserted_sets.append(set)

        set_exists = True
        # removing everything only on the first flush call
        if self._is_first_flush:
            self._delete_set_and_records(','.join([s['id'] for s in inserted_sets]))
            self._is_first_flush = False
            set_exists = False

        self._refresh_token()
        for dset in inserted_sets:
            if set_exists:
                r = self.session.patch(f'{self.api_url}/items/datasets/{dset["id"]}', json={"records": dset['records']})
            else:
                r = self.session.post(f'{self.api_url}/items/datasets', json=dset)
            r.raise_for_status()

        self._reset_cache()

    def _reset_cache(self):
        self._cache = {'records': {}, 'sets': {}, 'setrefs': {}}

    def _delete_set_and_records(self, sets_ids):
        # TODO Cascade deletion of setrefs for removed records (as for now setrefs collection hidden in Directus)
        # Removing records
        # as for now we rely on DBMS cascade in case of deletion on foreign key for setref table

        progress = ProgressBar()
        progress.animate('Removing records and datasets if already exists')

        self._refresh_token()
        r = self.session.get(f'{self.api_url}/items/datasets?filter[id][in]={sets_ids}&fields=records.record_id')

        for d in r.json()['data']:
            recs_ids = []
            recs = d['records']
            for i, r in enumerate(recs):
                recs_ids.append(r['record_id'])
                # since our ids are strings we get into situation with overlimiting url length and get 414 error
                if len(recs_ids) >= RECORDS_PER_REQUEST:
                    self.remove_record(','.join(recs_ids), False)
                    recs_ids.clear()
                    progress.tick(i, len(recs))
            if len(recs_ids) > 0:
                self.remove_record(','.join(recs_ids), False)
                progress.tick(i, len(recs))

        # TODO Removing sets?
        self.remove_set(sets_ids, False)

    def update_record(self, oai_id, modified, deleted, sets, metadata):
        # adds a record, call flush to actually store in db

        check_type(oai_id,
                   str,
                   prefix="record %s" % oai_id,
                   suffix='for parameter "oai_id"')
        check_type(modified,
                   datetime.datetime,
                   prefix="record %s" % oai_id,
                   suffix='for parameter "modified"')
        check_type(deleted,
                   bool,
                   prefix="record %s" % oai_id,
                   suffix='for parameter "deleted"')
        check_type(sets,
                   dict,
                   unicode_values=True,
                   recursive=True,
                   prefix="record %s" % oai_id,
                   suffix='for parameter "sets"')
        check_type(metadata,
                   dict,
                   prefix="record %s" % oai_id,
                   suffix='for parameter "metadata"')

        def date_handler(obj):
            if hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                raise TypeError('Object of type %s with value of %s is not JSON serializable' % (type(obj), repr(obj)))

        metadata = json.dumps(metadata, default=date_handler)
        self._cache['records'][oai_id] = (dict(modified=modified,
                                               deleted=deleted,
                                               metadata=metadata))
        self._cache['setrefs'][oai_id] = []
        for set_id in sets:
            self._cache['sets'][set_id] = dict(
                name=sets[set_id]['name'],
                description=sets[set_id].get('description'),
                hidden=sets[set_id].get('hidden', False))
            self._cache['setrefs'][oai_id].append(set_id)

    def get_record(self, oai_id):
        self._refresh_token()
        r = self.session.get(
            f'{self.api_url}/items/records?fields=id,deleted,modified,metadata,datasets.dataset_id&filter[id]={oai_id}')
        r.raise_for_status()

        recs = r.json()['data']
        if len(recs) > 0:
            return {'id': recs[0]['id'],
                    'deleted': recs[0]['deleted'],
                    'modified': datetime.datetime.strptime(recs[0]['modified'], DIRECTUS_DATETIME_FORMAT),
                    'metadata': json.loads(recs[0]['metadata']),
                    'sets': [set['dataset_id'] for set in recs[0]['datasets']]}
        return None

    def get_set(self, oai_id):
        self._refresh_token()
        r = self.session.get(
            f'{self.api_url}/items/datasets?fields=id,name,description,hidden&filter[id]={oai_id}&limit=1')
        r.raise_for_status()

        sets = r.json()['data']
        if len(sets) > 0:
            return {
                'id': sets[0]['id'],
                'name': sets[0]['name'],
                'description': sets[0]['description'],
                'hidden': sets[0]['hidden']}
        return None

    def get_setrefs(self, oai_id, include_hidden_sets=False):
        self._refresh_token()

        url = f'{self.api_url}/items/records?fields=datasets.dataset_id.id'

        filter_clause = f'&filter[id]={oai_id}'
        if not include_hidden_sets:
            filter_clause += '&filter[datasets.dataset_id.hidden]=0'

        r = self.session.get(url + filter_clause)
        r.raise_for_status()

        result = [record['dataset_id']['id'] for record in r.json()['data']]
        result.sort()

        return result

    def record_count(self):
        self._refresh_token()
        r = self.session.get(f'{self.api_url}/items/records?limit=0&meta=total_count')
        r.raise_for_status()

        return r.json()['meta']['total_count']

    def set_count(self):
        self._refresh_token()
        r = self.session.get(f'{self.api_url}/items/datasets?limit=0&meta=total_count')
        r.raise_for_status()

        return r.json()['meta']['total_count']

    def remove_record(self, oai_id, raise_for_status=True):
        self._refresh_token()
        r = self.session.delete(f'{self.api_url}/items/records/{oai_id}')
        if raise_for_status:
            r.raise_for_status()

    def remove_set(self, oai_id, raise_for_status=True):
        self._refresh_token()
        r = self.session.delete(f'{self.api_url}/items/datasets/{oai_id}')
        if raise_for_status:
            r.raise_for_status()

    def oai_sets(self, offset=0, batch_size=20):
        self._refresh_token()
        r = self.session.get(
            f'{self.api_url}/items/datasets?fields=id,name,description&filter[hidden]=0&offset={offset}&limit={batch_size}')
        r.raise_for_status()
        yield [{'id': set['id'],
                'name': set['name'],
                'description': ['description']} for set in r.json()['data']]

    def oai_earliest_datestamp(self):
        self._refresh_token()
        r = self.session.get(f'{self.api_url}/items/records?fields=modified&sort=modified')
        r.raise_for_status()

        return datetime.datetime.strptime(r.json()['data'][0]['modified'], DIRECTUS_DATETIME_FORMAT)

    def oai_query(self, offset=0, batch_size=20, needed_sets=None, disallowed_sets=None, allowed_sets=None,
                  from_date=None, until_date=None, identifier=None):
        self._refresh_token()

        needed_sets = needed_sets or []
        disallowed_sets = disallowed_sets or []
        allowed_sets = allowed_sets or []
        if batch_size < 0:
            batch_size = 0

        # make sure until date is set, and not in future
        if not until_date or until_date > datetime.datetime.utcnow():
            until_date = datetime.datetime.utcnow()

        url = f'{self.api_url}/items/records?fields=id,deleted,modified,metadata,datasets.dataset_id'
        filter_clause = f'&filter[id]={identifier}' if identifier is not None else ''
        filter_clause += f'&filter[modified][lte]={until_date.strftime(DIRECTUS_DATETIME_FORMAT)}'
        filter_clause += f'&filter[modified][gte]={from_date.strftime(DIRECTUS_DATETIME_FORMAT)}' if from_date is not None else ''

        if needed_sets:
            filter_clause += f'&filter[datasets.dataset_id][in]={",".join(needed_sets)}'

        if allowed_sets:
            filter_clause += f'&filter[datasets.dataset_id][in]={",".join(allowed_sets)}'

        if disallowed_sets:
            filter_clause += f'&filter[datasets.dataset_id][nin]={",".join(disallowed_sets)}'

        if offset:
            filter_clause += f'&offset={offset}'

        r = self.session.get(url + filter_clause)
        r.raise_for_status()

        for rec in r.json()['data']:
            yield {'id': rec['id'],
                   'deleted': rec['deleted'],
                   'modified': datetime.datetime.strptime(rec['modified'], DIRECTUS_DATETIME_FORMAT),
                   'metadata': json.loads(rec['metadata']),
                   'sets': [set['dataset_id'] for set in rec['datasets']]}
