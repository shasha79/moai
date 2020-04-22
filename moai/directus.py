import datetime
import json
import logging
import os
import re
import sqlalchemy as sql

import requests
from moai.database import SQLDatabase
from requests import HTTPError
from requests.adapters import HTTPAdapter
from urllib3.util import parse_url
from urllib3.util.retry import Retry

from moai.utils import check_type, ProgressBar

DIRECTUS_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DIRECTUS_API_PATTERN = '(directus://)((https?://)?(.*))'
RECORDS_PER_REQUEST = 150

logging.basicConfig(format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


class Directus:
    def __init__(self, dburi, config, email='', pwd='', user_id=None):
        match = re.match(DIRECTUS_API_PATTERN, dburi)
        if not match:
            raise Exception(f'{dburi}: Invalid Directus API URL given, should be of pattern {DIRECTUS_API_PATTERN}')

        self._reset_cache()

        self.direct_db = match.group(3) is None
        self.url = match.group(2)

        if self.direct_db:
            self.user_id = user_id
            db = self._db()
            self._records = db.tables['records']
            self._datasets = db.tables['datasets']
            self._datasetrefs = db.tables['datasetrefs']
        else:
            self.session = requests.Session()
            retry = Retry(
                backoff_factor=0.3,
                status_forcelist=(500, 503, 413, 429),
                method_whitelist=('POST', 'PATCH', 'OPTIONS', 'PUT', 'GET', 'TRACE', 'HEAD', 'DELETE')
            )
            self.session.mount(f'{parse_url(self.url).scheme}://', HTTPAdapter(max_retries=retry))
            self._is_first_flush = True

            self.staticToken = False
            if config and 'directus_auth_token' in config:
                self.staticToken = True
                self.session.headers.update({'Authorization': f'Bearer {config["directus_auth_token"]}'})

            self._refresh_token(config['directus_auth_email'] if config and 'directus_auth_email' in config else email,
                                config['directus_auth_pwd'] if config and 'directus_auth_pwd' in config else pwd)

    def _db(self):
        engine = sql.create_engine(self.url)
        db = sql.MetaData(engine)

        sql.Table('records', db,
                  sql.Column('id', sql.Unicode),
                  sql.Column('created_by', sql.INT),
                  sql.Column('modified', sql.DateTime),
                  sql.Column('deleted', sql.Boolean),
                  sql.Column('metadata', sql.Text))

        sql.Table('datasets', db,
                  sql.Column('id', sql.Unicode),
                  sql.Column('created_by', sql.INT),
                  sql.Column('hidden', sql.Boolean),
                  sql.Column('name', sql.Unicode),
                  sql.Column('description', sql.Unicode))

        sql.Table('datasetrefs', db,
                  sql.Column('created_by', sql.INT),
                  sql.Column('record_id', sql.Unicode, sql.ForeignKey('records.id')),
                  sql.Column('dataset_id', sql.Unicode, sql.ForeignKey('datasets.id')))

        return db

    def _reset_cache(self):
        self._cache = {'records': {}, 'sets': {}, 'setrefs': {}}

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

        auth_response = self.session.post(f'{self.url}/auth/{auth_route}', data=auth_data)
        if auth_response.status_code != 200:
            raise HTTPError(auth_response.json()['error']['message'], auth_response)

        self.session.headers.update({'Authorization': f'Bearer {auth_response.json()["data"]["token"]}'})

    def flush(self):
        if self.direct_db:
            self._db_flush()
        else:
            self._api_flush()

        self._reset_cache()

    def _db_flush(self):
        oai_ids = set()
        for row in sql.select([self._records.c.id]).execute():
            oai_ids.add(row[0])
        for row in sql.select([self._datasets.c.id]).execute():
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
            item['created_by'] = self.user_id
            inserted_records.append(item)

        for oai_id, item in list(self._cache['sets'].items()):
            if oai_id in oai_ids:
                # set allready exists
                deleted_sets.append(oai_id)
            item['id'] = oai_id
            item['created_by'] = self.user_id
            item['description'] = oai_id
            inserted_sets.append(item)

        for record_id, set_ids in list(self._cache['setrefs'].items()):
            deleted_setrefs.append(record_id)
            for set_id in set_ids:
                inserted_setrefs.append(
                    {'created_by': self.user_id, 'record_id': record_id, 'dataset_id': set_id})

        # delete all processed records before inserting
        if deleted_records:
            self._records.delete(
                self._records.c.id == sql.bindparam('id')
            ).execute(
                [{'id': rid} for rid in deleted_records])
        if deleted_sets:
            self._datasets.delete(
                self._datasets.c.id == sql.bindparam('id')
            ).execute(
                [{'id': sid} for sid in deleted_sets])
        if deleted_setrefs:
            self._datasetrefs.delete(
                self._datasetrefs.c.record_id == sql.bindparam('record_id')
            ).execute(
                [{'record_id': rid} for rid in deleted_setrefs])

        # batch inserts
        if inserted_records:
            self._records.insert().execute(inserted_records)
        if inserted_sets:
            self._datasets.insert().execute(inserted_sets)
        if inserted_setrefs:
            self._datasetrefs.insert().execute(inserted_setrefs)

    def _api_flush(self):
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
                r = self.session.patch(f'{self.url}/items/datasets/{dset["id"]}', json={"records": dset['records']})
            else:
                r = self.session.post(f'{self.url}/items/datasets', json=dset)
            r.raise_for_status()
            log.info(f'Flush took {r.elapsed}')

    def _delete_set_and_records(self, sets_ids):
        # TODO Cascade deletion of setrefs for removed records (as for now setrefs collection hidden in Directus)
        # Removing records
        # as for now we rely on DBMS cascade in case of deletion on foreign key for setref table

        progress = ProgressBar()
        progress.animate('Removing records and datasets if already exists')

        self._refresh_token()
        r = self.session.get(f'{self.url}/items/datasets?filter[id][in]={sets_ids}&fields=records.record_id')

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
            f'{self.url}/items/records?fields=id,deleted,modified,metadata,datasets.dataset_id&filter[id]={oai_id}')
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
            f'{self.url}/items/datasets?fields=id,name,description,hidden&filter[id]={oai_id}&limit=1')
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

        url = f'{self.url}/items/records?fields=datasets.dataset_id.id'

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
        r = self.session.get(f'{self.url}/items/records?limit=0&meta=total_count')
        r.raise_for_status()

        return r.json()['meta']['total_count']

    def set_count(self):
        self._refresh_token()
        r = self.session.get(f'{self.url}/items/datasets?limit=0&meta=total_count')
        r.raise_for_status()

        return r.json()['meta']['total_count']

    def remove_record(self, oai_id, raise_for_status=True):
        self._refresh_token()
        r = self.session.delete(f'{self.url}/items/records/{oai_id}')
        if raise_for_status:
            r.raise_for_status()

    def remove_set(self, oai_id, raise_for_status=True):
        self._refresh_token()
        r = self.session.delete(f'{self.url}/items/datasets/{oai_id}')
        if raise_for_status:
            r.raise_for_status()

    def oai_sets(self, offset=0, batch_size=20):
        self._refresh_token()
        r = self.session.get(
            f'{self.url}/items/datasets?fields=id,name,description&filter[hidden]=0&offset={offset}&limit={batch_size}')
        r.raise_for_status()
        for set in r.json()['data']:
            yield {'id': set['id'],
                   'name': set['name'],
                   'description': set['description']} 

    def oai_earliest_datestamp(self):
        self._refresh_token()
        r = self.session.get(f'{self.url}/items/records?fields=modified&sort=modified')
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

        url = f'{self.url}/items/records?fields=id,deleted,modified,metadata,dataset_id'
        filter_clause = f'&filter[id]={identifier}' if identifier is not None else ''
        filter_clause += f'&limit={batch_size}'
        filter_clause += f'&filter[modified][lte]={until_date.strftime(DIRECTUS_DATETIME_FORMAT)}'
        filter_clause += f'&filter[modified][gte]={from_date.strftime(DIRECTUS_DATETIME_FORMAT)}' if from_date is not None else ''

        if needed_sets:
            filter_clause += f'&filter[dataset_id][in]={",".join(needed_sets)}'

        if allowed_sets:
            filter_clause += f'&filter[dataset_id][in]={",".join(allowed_sets)}'

        if disallowed_sets:
            filter_clause += f'&filter[dataset_id][nin]={",".join(disallowed_sets)}'

        if offset:
            filter_clause += f'&offset={offset}'

        r = self.session.get(url + filter_clause)
        r.raise_for_status()

        for rec in r.json()['data']:
            yield {'id': rec['id'],
                   'deleted': rec['deleted'],
                   'modified': datetime.datetime.strptime(rec['modified'], DIRECTUS_DATETIME_FORMAT),
                   'metadata': json.loads(rec['metadata']),
                   'sets': [rec['dataset_id']]}
