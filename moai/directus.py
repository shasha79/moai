import datetime
import json

import requests
from moai.utils import check_type


class Directus():
    def __init__(self, config):
        self.api_url = config['api_url']
        self.session = requests.Session()
        self._connect(config['api_auth_email'], config['api_auth_pass'])
        self._reset_cache()

    def _connect(self, email, pwd):
        auth_response = self.session.post(f'{self.api_url}/auth/authenticate',
                                          data={'email': email, 'password': pwd})
        self.token = auth_response.json()["data"]["token"]

    def _refresh_token(self):
        auth_response = self.session.post(f'{self.api_url}/auth/refresh', data={'token': self.token})
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
            set['records'] = []  # [{'record_id': record} for record in inserted_records]
            inserted_sets.append(set)

        # TODO implement deletion of existing records, sets and their mappings
        # self._refresh_token()
        # r = self.session.get(f'{self.api_url}/items/sets?filter[id][in]={",".join([set["id"] for set in inserted_sets])}&fields=records.record_id')
        # print(r.json()['data'])

        self._refresh_token()
        r = self.session.post(f'{self.api_url}/items/sets', json=inserted_sets)
        r.raise_for_status()

        self._reset_cache()

    def _reset_cache(self):
        self._cache = {'records': {}, 'sets': {}, 'setrefs': {}}

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
        raise NotImplementedError('Not yet implemented')

    def get_set(self, oai_id):
        raise NotImplementedError('Not yet implemented')

    def get_setrefs(self, oai_id, include_hidden_sets=False):
        raise NotImplementedError('Not yet implemented')

    def record_count(self):
        raise NotImplementedError('Not yet implemented')

    def set_count(self):
        raise NotImplementedError('Not yet implemented')

    def remove_record(self, oai_id):
        raise NotImplementedError('Not yet implemented')

    def remove_set(self, oai_id):
        raise NotImplementedError('Not yet implemented')

    def oai_sets(self, offset=0, batch_size=20):
        raise NotImplementedError('Not yet implemented')

    def oai_earliest_datestamp(self):
        raise NotImplementedError('Not yet implemented')

    def oai_query(self, offset=0, batch_size=20, needed_sets=None, disallowed_sets=None, allowed_sets=None,
                  from_date=None, until_date=None, identifier=None):
        raise NotImplementedError('Not yet implemented')
