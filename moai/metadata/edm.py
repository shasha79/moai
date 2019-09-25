
from lxml import etree
import xmltodict

XSI_NS = 'http://www.w3.org/2001/XMLSchema-instance'
  
class EDM(object):
    """The standard EDM metadata format.
    

    It is registered under the name 'oai_dc'
    """
    
    def __init__(self, prefix, config, db):
        self.prefix = prefix
        self.config = config
        self.db = db

        self.ns = {'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
                   'dc':'http://purl.org/dc/elements/1.1/',
                   'dcterms':'http://purl.org/dc/terms/',
                   'dct': 'http://purl.org/dc/terms/',
                   'edm' : 'http://www.europeana.eu/schemas/edm/',
                   'foaf': 'http://xmlns.com/foaf/0.1/',
                   'owl' : 'http://www.w3.org/2002/07/owl#',
                   'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
                   'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
                   'skos': 'http://www.w3.org/2004/02/skos/core#',
                   'xsi' : 'http://www.w3.org/2001/XMLSchema-instance',
                   'ore': 'http://www.openarchives.org/ore/terms/',
                   }
        self.schemas = {
            'edm': 'http://www.europeana.eu/schemas/edm/EDM-EXTERNAL-MAIN.xsd'}
        
    def get_namespace(self):
        return self.ns[self.prefix]

    def get_schema_location(self):
        return self.schemas[self.prefix]
    
    def __call__(self, element, metadata):
        data = metadata.record
        if not data['metadata']: return
        for prefix, ns in self.ns.items():
            data['metadata']['rdf:RDF']['@xmlns:{}'.format(prefix)] = ns

        metadata_unparsed = xmltodict.unparse(data['metadata'], full_document=False)
        e = etree.fromstring(metadata_unparsed)
        element.append(e)
