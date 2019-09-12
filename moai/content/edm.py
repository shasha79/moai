
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
        self.modified= datetime.datetime.now()-datetime.timedelta(days=7)
        self.deleted = False
        self.metadata = edm
        self.sets = {"edm": {"name": b"EDM Sample",
                             "desciption": b"EDM Sample"}}