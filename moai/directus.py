import datetime
import requests
import json
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
        self._refresh_token()
        oai_ids = set()
        records_response = self.session.get(f'{self.api_url}/items/records?fields=id')
        for row in records_response.json()['data']:
            oai_ids.add(row[0])
        sets_response = self.session.get(f'{self.api_url}/items/sets?fields=id')
        for row in sets_response.json()['data']:
            oai_ids.add(row[0])

        deleted_records = []
        deleted_sets = []
        deleted_setrefs = []

        inserted_records = []
        inserted_sets = []
        inserted_setrefs = []


        for oai_id, item in list(self._cache['records'].items()):
            if oai_id in oai_ids:
                # record allready exists
                deleted_records.append(oai_id)
            item['id'] = oai_id
            item['modified'] = item['modified'].isoformat()
            inserted_records.append(item)

        for oai_id, item in list(self._cache['sets'].items()):
            if oai_id in oai_ids:
                # set allready exists
                deleted_sets.append(oai_id)
            item['id'] = oai_id
            inserted_sets.append(item)

        for record_id, set_ids in list(self._cache['setrefs'].items()):
            deleted_setrefs.append(record_id)
            for set_id in set_ids:
                inserted_setrefs.append(
                    {'record_id':record_id, 'set_id': set_id})

        # delete all processed records before inserting
        if deleted_records:
            pass
            # self._records.delete(
            #     self._records.c.record_id == sql.bindparam('record_id')
            # ).execute(
            #     [{'record_id': rid} for rid in deleted_records])
        if deleted_sets:
            pass
            # self._sets.delete(
            #     self._sets.c.set_id == sql.bindparam('set_id')
            # ).execute(
            #     [{'set_id': sid} for sid in deleted_sets])
        if deleted_setrefs:
            pass
            # self._setrefs.delete(
            #     self._setrefs.c.record_id == sql.bindparam('record_id')
            # ).execute(
            #     [{'record_id': rid} for rid in deleted_setrefs])

        # batch inserts
        if inserted_records:
            r = self.session.post(f'{self.api_url}/items/records', json=inserted_records)
            # self._records.insert().execute(inserted_records)
        if inserted_sets:
            pass
            # self._sets.insert().execute(inserted_sets)
        if inserted_setrefs:
            pass
            # self._setrefs.insert().execute(inserted_setrefs)

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
                name = sets[set_id]['name'],
                description = sets[set_id].get('description'),
                hidden = sets[set_id].get('hidden', False))
            self._cache['setrefs'][oai_id].append(set_id)


    def get_record(self, oai_id):
        pass

    def get_set(self, oai_id):
        pass

    def get_setrefs(self, oai_id, include_hidden_sets=False):
        pass

    def record_count(self):
        pass

    def set_count(self):
        pass

    def remove_record(self, oai_id):
        pass

    def remove_set(self, oai_id):
        pass

    def oai_sets(self, offset=0, batch_size=20):
        pass

    def oai_earliest_datestamp(self):
        pass

    def oai_query(self, offset=0, batch_size=20, needed_sets=None, disallowed_sets=None, allowed_sets=None,
                  from_date=None, until_date=None, identifier=None):
        pass
