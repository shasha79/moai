import os

from zope.interface import implementer
from lxml import etree
import xmltodict

from moai.interfaces import IContentProvider
from moai.provider.file import FileBasedContentProvider

@implementer(IContentProvider)
class EdmBasedContentProvider(FileBasedContentProvider):
    """Providers content by loading paged EDM files.
    Implements the :ref:`IContentProvider` interface
    """


    def __init__(self, uri, content_filter="*"):
        super(EdmBasedContentProvider, self).__init__(uri.replace("edm://", "file://"), content_filter)

    def set_logger(self, log):
        self._log = log

    def update(self, from_date=None):
        self._log.info('Loading EDM files from: %s' % self._path)

        edm_files = super(EdmBasedContentProvider, self).update()
        self._content.clear()

        for edm_file in edm_files:
            with open(os.path.join(self._path, edm_file), 'rb') as ef:
                print(ef)
                root = xmltodict.parse(ef.read(), process_namespaces=False)
                for cho in root["OAI-PMH"]["ListRecords"]["record"]:
                    # TODO: take key from record/header/identifier instead of rdf:about
                    self._content[cho["metadata"]["rdf:RDF"]["edm:ProvidedCHO"]["@rdf:about"]] = cho["metadata"]
                    if self._set:
                        if not cho["header"]: cho["header"] = dict()
                        cho["header"]["setSpec"] = self._set
                    yield cho

    def _get_id(self, header):
        return header.identifier()

if __name__ == "__main__":
    import sys, logging
    cp = EdmBasedContentProvider(sys.argv[1], sys.argv[2])
    cp.set_logger(logging.getLogger())
    for cho in cp.update():
        print(cho)