
from lxml import etree

from moai.utils import XPath

import datetime
class EdmContent(object):
    def __init__(self, provider):
        self.provider = provider
        self.id = None
        self.modified = None
        self.deleted = None
        self.sets = None
        self.metadata = None

    def update(self, edm):
        self.id = edm["rdf:RDF"]["edm:ProvidedCHO"]["@rdf:about"]
        self.modified= datetime.datetime.now()-datetime.timedelta.days(365) # TODO: read from header of EDM record
        self.deleted = False  # TODO: support deletions
        self.metadata = edm

        set = self.provider.get_set()
        self.sets = {set: {"name": bytes(set, 'utf-8'),
                             "desciption": b"EDM Feed"}}