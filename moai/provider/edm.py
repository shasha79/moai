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
                    yield cho
                # context = etree.iterparse(ef, events=('end',),
                #                           tag='{http://www.europeana.eu/schemas/edm/}ProvidedCHO')
                # try:
                #     i = 0
                #     for cho in self._iterate(context):  # If found any invalid part in xml, stop the process
                #         if not i % 5:
                #             self._log.warning("Parsed {} objects".format(i))
                #             self._log.info("Sample object: {}".format(cho))
                #         i += 1
                #         yield cho
                # except etree.XMLSyntaxError:  # check if file is well formed
                #     self._log.info('Skipping invalid XML {}'.format(edm_file))

    # def _iterate(self, context):
    #     # Extract from --> http:/text/www.ibm.com/developerworks/xml/library/x-hiperfparse/
    #     print(context)
    #     for event, elem in context:
    #         print(event)
    #         for cho in self._parse_cho(elem):
    #             yield cho
    #         elem.clear()
    #         while elem.getprevious() is not None:
    #             del elem.getparent()[0]
    #     del context
    #
    # def _parse_cho(self, element):
    #     cho_dict = {}
    #     print(element)
    #     cho_dict['id'] = element.attrib.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about')
    #     cho_dict['metadata'] = xmltodict.parse(element, process_namespaces=True)
    #     return cho_dict

    def _get_id(self, header):
        return header.identifier()

if __name__ == "__main__":
    import sys, logging
    cp = EdmBasedContentProvider(sys.argv[1], sys.argv[2])
    cp.set_logger(logging.getLogger())
    for cho in cp.update():
        print(cho)