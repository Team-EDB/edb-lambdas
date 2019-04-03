import requests
import os
from xml.etree import ElementTree as xml

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
#from gremlin_python.process.graph_traversal import __
from gremlin_python import statics

statics.load_statics(globals()) #I hate this

db = os.environ.get('EDB_DB', "wss://biosurvdbtest.cvkyaz9id4ml.us-east-1.neptune.amazonaws.com:8182/gremlin")
api_key = os.environ.get('NCBI_API_KEY', "")

if api_key:
    print(f"API key found in environment: {api_key}")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}&api_key={api_key}"
else:
    print("No API key found")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}"
    
    
isolate = dict()
sample = dict()
    
    
def convert_record(biorecord, taxrecord, g):
    """Convert Biosample record and Taxonomy record into a series of associated
       records that capture taxonomy, sample collection, and a lot of names"""
    primary_id = ""
    for id_ in biorecord.findall('.//Ids/Id'):
        if id_.attrib.get('is_primary'):
            primary_id = id_.text
            break
    #create the sample
    s = ( g.V().has('sample', 'sample_id', primary_id).fold()
          .coalesce(
            unfold(),
            addV('metadata::sample', 'sample_id', primary_id))
        )
    s = s.property('package', biorecord.find('.//Package').text)
    for prop in biorecord.findall('.//Attribute'):
        s = s.property(prop.attrib['attribute_name'], prop.text)
    #s = s.next()
    #return s
    #print(s)
    
    #traverse the taxonomic tree and create nodes if they don't exist
    r = None
    # taxid, sci_name, _, _, rank, *_ = taxrecord.find('./TaxaSet/Taxon')
    # sp = (
    #         g.V().has('taxon', 'taxid', taxid).fold()
    #          .coalesce(unfold(),
    #                   addV('taxonomy::taxon')
    #                   .property('name', sci_name.split()[-1])
    #                   .property('rank', rank)
    #                   .property('taxid', taxid))
    #      )
    
    
    # for taxid, name, rank in taxrecord.findall(".//LineageEx/Taxon"):
    #     taxid = taxid.text
    #     name = name.text.split(' ')[-1] #tokenize and get the last token
    #     rank = rank.text
    #     t = (
    #         g.V().has('taxon', 'taxid', taxid).fold() 
    #         .coalesce(unfold(),                       
    #                   addV('taxonomy::taxon')                   
    #                   .property('name', name)         
    #                   .property('rank', rank)         
    #                   .property('taxid', taxid))
    #         )
    #     #print(t)
    #     if r:
    #         #print(r.propertyMap()['taxid']) ###
    #         (
    #          t.coalesce(t.outE('IS_A').to(r),
    #                     addE('IS_A').from_(t).to(r))
    #         )
    #     r = t
    # #print(r)
    s = (
     g.V().coalesce(s.outE('IS_A').to(r),
                    addE('IS_A').from_(s).to(r))
    )
     
    #load names
    for name_element in biorecord.find('.//Ids'):
        name = name_element.text
        namespace = name_element.attrib['db']
        s = (
         g.V().coalesce(
             s.outE('NAMED_IN').where().property('name', name).to(
                 coalesce(
                     g.V().has('namespace', 'namespace', namespace),
                     g.addV('metadata::namespace', 'namespace', namespace)
                     ).as_('ns')
                ),
             s.addE('NAMED_IN', 'name', name).to('ns')
             )
        )
    s.next()  #execute
    print(s)


def lambda_handler(event, context):
    #print(event['biosample'])
    graph = Graph()
    g = graph.traversal().withRemote(DriverRemoteConnection(db, 'g'))
    record = xml.fromstring(
        requests.get(
            req.format(database="biosample", 
                       accession=event['biosample'], 
                       api_key=api_key)).text)
                       
    taxid = record.find('.//Organism').attrib['taxonomy_id']
                       
    taxon = xml.fromstring(
        requests.get(
            req.format(database="taxonomy",
                       accession=taxid,
                       api_key=api_key)).text)
    #print(xml.tostring(taxon))
    #print(record, taxon)
    convert_record(record, taxon, g)
    #print(xml.tostring(record))
    return ''