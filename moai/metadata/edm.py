
from lxml import etree
import xmltodict
import json
from collections import OrderedDict


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
                   'svcs': 'http://rdfs.org/sioc/services#',
                   'doap': 'http://usefulinc.com/ns/doap#',
                   'rdaGr2': 'http://rdvocab.info/ElementsGr2/'
                   }
        self.schemas = {
            'edm': 'http://www.europeana.eu/schemas/edm/EDM-EXTERNAL-MAIN.xsd'}
        
    def get_namespace(self):
        return self.ns[self.prefix]

    def get_schema_location(self):
        return self.schemas[self.prefix]
      
    def fix_ordering_of_edm_elements(self, rdf):
        edm_provider_cho_order = [
            "dc:contributor",
            "dc:coverage",
            "dc:creator",
            "dc:date",
            "dc:description",
            "dc:format",
            "dc:identifier",
            "dc:language",
            "dc:publisher",
            "dc:relation",
            "dc:rights",
            "dc:source",
            "dc:subject",
            "dc:title",
            "dc:type",
            "dcterms:alternative",
            "dcterms:conformsTo",
            "dcterms:created",
            "dcterms:extent",
            "dcterms:hasFormat",
            "dcterms:hasPart",
            "dcterms:hasVersion",
            "dcterms:isFormatOf",
            "dcterms:isPartOf",
            "dcterms:isReferencedBy",
            "dcterms:isReplacedBy",
            "dcterms:isRequiredBy",
            "dcterms:issued",
            "dcterms:isVersionOf",
            "dcterms:medium",
            "dcterms:provenance",
            "dcterms:references",
            "dcterms:replaces",
            "dcterms:requires",
            "dcterms:spatial",
            "dcterms:tableOfContents",
            "dcterms:temporal",
            "edm:currentLocation",
            "edm:hasMet",
            "edm:hasType",
            "edm:incorporates",
            "edm:isDerivativeOf",
            "edm:isNextInSequence",
            "edm:isRelatedTo",
            "edm:isRepresentationOf",
            "edm:isSimilarTo",
            "edm:isSuccessorOf",
            "edm:realizes",
            "edm:type",
            "owl:sameAs",
        ]
        web_resource_order = [
            "dc:creator",
            "dc:description",
            "dc:format",
            "dc:rights",
            "dc:source",
            "dc:type",
            "dcterms:confromsTo",
            "dcterms:created",
            "dcterms:extent",
            "dcterms:hasPart",
            "dcterms:isFormatOf",
            "dcterms:isPartOf",
            "dcterms:isReferencedBy",
            "dcterms:issued",
            "edm:isNextInSequence",
            "edm:rights",
            "owl:sameAs",
            "svcs:has_service",
        ]
        ore_agg_order = [
            "edm:aggregatedCHO",
            "edm:dataProvider",
            "edm:hasView",
            "edm:isShownAt",
            "edm:isShownBy",
            "edm:object",
            "edm:provider",
            "dc:rights",
            "edm:rights",
            "edm:ugc",
        ]
        edm_agent_order = [
            "skos:prefLabel",
            "skos:altLabel",
            "skos:note",
            "dc:date",
            "dc:identifier",
            "dcterms:hasPart",
            "dcterms:isPartOf",
            "edm:begin",
            "edm:end",
            "edm:hasMet",
            "edm:isRelatedTo",
            "foaf:name",
            "rdaGr2:biographicalInformation",
            "rdaGr2:dateOfBirth",
            "rdaGr2:dateOfDeath",
            "rdaGr2:dateOfEstablishment",
            "rdaGr2:dateOfTermincation",
            "rdaGr2:gender",
            "rdaGr2:placeOfBirth",
            "rdaGr2:placeOfDeath",
            "rdaGr2:professionOrOccupation",
            "owl:sameAs",
        ]
        edm_place_order = [
            "wgs84_pos:lat",
            "wgs84_pos:long",
            "wgs84_pos:alt",
            "skos:prefLabel",
            "skos:altLabel",
            "skos:note",
            "dcterms:hasPart",
            "dcterms:isPartOf",
            "edm:isNextInSequence",
            "owl:sameAs",
        ]
        edm_timespan_order = [
            "skos:prefLabel",
            "skos:altLabel",
            "skos:note",
            "dcterms:hasPart",
            "dcterms:isPartOf",
            "edm:begin",
            "edm:end",
            "edm:isNextInSequence",
            "owl:sameAs",
        ]
        skos_concept_order = [
            "skos:prefLael",
            "skos:altLabel",
            "skos:broader",
            "skos:narrower",
            "skos:related",
            "skos:broadMatch",
            "skos:narrowMatch",
            "skos:relatedMatch",
            "skos:exactMatch",
            "skos:closeMatch",
            "skos:note",
            "skos:notation",
            "skos:inScheme",
        ]
        cc_license_order = ["odrl:inheritFrom", "cc:depreceatedOn"]
        svcs_service_order = ["dcterms:conformsTo", "doap:implements"]

        rdf_key_order_mapping = {
            "edm:ProvidedCHO": edm_provider_cho_order,
            "edm:WebResource": web_resource_order,
            "ore:Aggregation": ore_agg_order,
            "edm:Agent": edm_agent_order,
            "edm:Place": edm_place_order,
            "edm:TimeSpan": edm_timespan_order,
            "skos:Concept": skos_concept_order,
            "cc:License": cc_license_order,
            "svcs:Service": svcs_service_order,
        }

        for rdf_key in rdf.keys():
            if rdf_key in rdf_key_order_mapping.keys():
                for key in rdf_key_order_mapping[rdf_key]:
                    if isinstance(rdf[rdf_key], list):
                        for n, contextual_class in enumerate(rdf[rdf_key]):
                            if key in contextual_class.keys():
                                rdf[rdf_key][n].move_to_end(key, last=True)
                    else:
                        if key in rdf[rdf_key].keys():
                            rdf[rdf_key].move_to_end(key, last=True)
    
    def __call__(self, element, metadata):
        data = metadata.record
        if not data['metadata']: return
        for prefix, ns in self.ns.items():
            data['metadata']['rdf:RDF']['@xmlns:{}'.format(prefix)] = ns

        data['metadata']['rdf:RDF'] = json.loads(json.dumps(data['metadata']['rdf:RDF']),object_pairs_hook=OrderedDict)
        self.fix_ordering_of_edm_elements(data['metadata']['rdf:RDF'])
        
        metadata_unparsed = xmltodict.unparse(data['metadata'], full_document=False)
        e = etree.fromstring(metadata_unparsed)
        element.append(e)
