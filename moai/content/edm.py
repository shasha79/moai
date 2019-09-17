
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
        self.modified= datetime.datetime.now() # TODO: read from header of EDM record
        self.deleted = False  # TODO: support deletions
        self.metadata = edm
        self.sets = {"edm": {"name": b"EDM Sample",
                             "desciption": b"EDM Sample"}}