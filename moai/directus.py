import datetime
import json
import logging
import re
from sqlalchemy.engine import make_url

from jhn.directus.client import Directus
DIRECTUS_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
DIRECTUS_API_PATTERN = '(directus://)((https?://)?(.*))'


logging.basicConfig(
    format='%(asctime)s:%(name)s:%(levelname)s:%(message)s', level=logging.INFO)
log = logging.getLogger(__name__)


class DirectusProvider:
    def __init__(self, dburi, config):
        match = re.match(DIRECTUS_API_PATTERN, dburi)
        if not match:
            raise Exception(
                f'{dburi}: Invalid Directus API URL given, should be of pattern {DIRECTUS_API_PATTERN}')

        self.sql_url = make_url(match.group(3))
        self.directus = Directus(config['directus_url'], access_token=config['directus_access_token'])

    def get_record(self, oai_id):
        recs = self.directus.get_items("items", item_id=oai_id)

        if len(recs) > 0:
            return {'id': recs[0]['id'],
                    'deleted': False,
                    'modified': datetime.datetime.strptime(recs[0]['date_updated'], DIRECTUS_DATETIME_FORMAT),
                    'metadata': json.loads(recs[0]['metadata']),
                    'sets': [recs[0]['dataset']]}
        return None

    def get_set(self, oai_id):
        sets = self.directus.get_items("datasets", item_id=oai_id)
        if len(sets) > 0:
            return {
                'id': sets[0]['id'],
                'name': sets[0]['name'],
                'description': sets[0]['description'],
                'hidden': False}
        return None

    def get_setrefs(self, oai_id, include_hidden_sets=False):
        # TODO: implement (if needed)
        pass

    def record_count(self):
        return self.directus.count_items("items")

    def set_count(self):
        pass

    def remove_record(self, oai_id, raise_for_status=True):
        return self.directus.delete_item("items", oai_id)

    def remove_set(self, oai_id, raise_for_status=True):
        return self.directus.delete_item("datasets", oai_id)

    def oai_sets(self, offset=0, batch_size=20):
        datasets = self.directus.get_items("datasets", offset=offset, limit=batch_size)
        for set in datasets:
            yield {'id': set['id'],
                   'name': set['name'],
                   'description': set['description']}

    def oai_earliest_datestamp(self):
        datasets = self.directus.get_items("datasets", fields=['date_updated'], sort='date_updated')
        return datetime.datetime.strptime(datasets[0]['date_updated'], DIRECTUS_DATETIME_FORMAT)

    def oai_query(self, offset=0, batch_size=20, needed_sets=[], disallowed_sets=[], allowed_sets=[],
                  from_date=None, until_date=None, identifier=None):

        needed_sets = needed_sets or []
        disallowed_sets = disallowed_sets or []
        allowed_sets = allowed_sets or []
        if batch_size < 0:
            batch_size = 0

        # make sure until date is set, and not in future
        if not until_date or until_date > datetime.datetime.utcnow():
            until_date = datetime.datetime.utcnow()

        filter_params = {}
        if identifier:
            filter_params['filter[id][_eq]'] = identifier

        #url = f'{self.url}/items/records?fields=id,deleted,modified,metadata,dataset_id'
        #filter_clause = f'&filter[id]={identifier}' if identifier is not None else ''
        #filter_clause += f'&limit={batch_size}'

        #filter_clause += f'&filter[modified][lte]={until_date.strftime(DIRECTUS_DATETIME_FORMAT)}'
#        filter_clause += f'&filter[modified][gte]={from_date.strftime(DIRECTUS_DATETIME_FORMAT)}' if from_date is not None else ''
        filter_params['filter[date_updated][_lte]'] = f'{until_date.strftime(DIRECTUS_DATETIME_FORMAT)}'
        if from_date:
            filter_params['filter[date_updated][_gte]'] = f'{from_date.strftime(DIRECTUS_DATETIME_FORMAT)}'

        in_sets = set()
        in_sets = in_sets.union(allowed_sets)
        in_sets = in_sets.union(needed_sets)
        if in_sets:
            filter_params['filter[dataset][id][_in]'] = f'{",".join(in_sets)}'

        if disallowed_sets:
            #filter_clause += f'&filter[dataset_id][nin]={",".join(disallowed_sets)}'
            filter_params['filter[dataset][id][_nin]'] = f'{",".join(disallowed_sets)}'


        records = self.directus.get_items("items", offset=offset, limit=batch_size, **filter_params)

        for rec in records:
            yield {'id': rec['id'],
                   'deleted': (rec['status'] == 'archived'),
                   'modified': datetime.datetime.strptime(rec['date_updated'], DIRECTUS_DATETIME_FORMAT),
                   'metadata': rec['metadata'],
                   'sets': [rec['dataset']]}
