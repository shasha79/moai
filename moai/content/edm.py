import dateparser
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
        self.id = edm["header"]["identifier"]
        self.modified= dateparser.parse(edm["header"]["datestamp"])
        self.deleted = False  # TODO: support deletions
        self.metadata = edm.get("metadata", dict())

        set = self.provider.get_set()
        self.sets = {set: {"name": bytes(set, 'utf-8'),
                             "desciption": b"EDM Feed"}}