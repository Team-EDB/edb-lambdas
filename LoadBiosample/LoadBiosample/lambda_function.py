import boto3
import json
import requests
import os
from xml.etree import ElementTree as xml

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import __
from gremlin_python import statics

statics.load_statics(globals()) #I hate this

db = os.environ.get('EDB_DB', "wss://biosurvdbtest.cvkyaz9id4ml.us-east-1.neptune.amazonaws.com:8182/gremlin")
api_key = os.environ.get('NCBI_API_KEY', "")
sra_queue = os.environ.get('SRA_QUEUE', 'edb-sra')

if api_key:
    print(f"API key found in environment: {api_key}")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}&api_key={api_key}"
else:
    print("No API key found")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}"
    
sqs = boto3.resource("sqs", region_name=os.environ.get("SERVICE_REGION", "us-east-1"))
sraq = sqs.get_queue_by_name(QueueName=sra_queue)
    
    
isolate = dict()
sample = dict()
    
    
def convert_record(biorecord, taxrecord, g):
    """Convert Biosample record and Taxonomy record into a series of associated
       records that capture taxonomy, sample collection, and a lot of names"""
    primary_id = biorecord.find('./BioSample').attrib['accession']
    #create the sample
    s = ( g.V().has('sample', 'sample_id', primary_id).fold()
          .coalesce(
            unfold(),
            addV('metadata::sample').property('sample_id', primary_id))
        )
    s = s.property('package', biorecord.find('.//Package').text)
    for prop in biorecord.findall('.//Attribute'):
        s = s.property(prop.attrib['attribute_name'], prop.text)
    s = s.next() #commit
    #return s
    #print(s)
    
    #traverse the taxonomic tree and create nodes if they don't exist
    #most everything points outward from the sample, to put the
    #sample at the top of a DAG.
    r = None
    try:
        taxid, sci_name, _, _, rank, *_ = taxrecord.find('./Taxon')
    except TypeError: #root taxon not found
        pass
    else:
        sp = (
                g.V().has('taxon', 'taxid', taxid.text).fold()
                 .coalesce(unfold(),
                           addV('taxonomy::taxon')
                           .property('name', sci_name.text.split()[-1])
                           .property('rank', rank.text)
                           .property('taxid', taxid.text))
             ).next() #commit

    #traverse the LineageEx and update our taxonomy tree
    
    for taxid, name, rank in taxrecord.findall(".//LineageEx/Taxon"):
        taxid = taxid.text
        name = name.text.split(' ')[-1] #tokenize and get the last token
        rank = rank.text
        t = (
            g.V().has('taxon', 'taxid', taxid).fold() 
            .coalesce(unfold(),                       
                      addV('taxonomy::taxon')                   
                      .property('name', name)         
                      .property('rank', rank)         
                      .property('taxid', taxid))
            ).next() #commit
        #print(t)
        if r:
            #t -[is_a]-> r
            (
             g.V(t).as_('t')
              .V(r).coalesce(
                    __.inE('IS_A').where(outV().as_('t')),
                    addE('IS_A').from_('t')
                 )
            ).next() #commit
        r = t
    #print(r)
    #some tax records don't extend the lineage all the way to the parent taxon
    #this captures any enclosing terminal taxon from the other part of the
    #xml record.
    (
     g.V(sp).as_('sp')
      .V(t).coalesce(
            __.inE('IS_A').where(outV().as_('sp')),
               addE('IS_A').from_('sp')
            )
    ).next() #commit
    
    #lastly, connect the sample itself to its taxonomy.
    (
     g.V(s).as_('s')
      .V(sp).coalesce(
           __.inE('IS_A').where(outV().as_('s')),
              addE('IS_A').from_('s')
          )
    ).next()
     
    #load sample names
    for name_element in biorecord.find('.//Ids'):
        name = name_element.text
        namespace = name_element.attrib['db']
        ns = (
          g.V().has('namespace', 'name', namespace).fold()
            .coalesce(
               unfold(),
               addV('metadata::namespace').property('name', namespace)
            ).V().has('namespace', 'name', namespace).as_('ns')
             .V(s)
            .coalesce(
                __.inE('NAMED_IN').where(inV().as_('ns')
                                  .and_()
                                  .values('name').is_(name)),
                  addE('NAMED_IN')
                    .to('ns')
                    .property('name', name)
            )
        ).next()
        # (
        #  g.V(s).outE('NAMED_IN').where('name', name).to(
        #      coalesce(
        #          g.V().has('namespace', 'namespace', namespace),
        #          g.addV('metadata::namespace', 'namespace', namespace)
        #          ).as_('ns')
        #     ).fold()
        #   .coalesce(
        #         unfold(),
        #         addE('NAMED_IN').to('ns').property('name', name)
        #     )
        # ).next()  #commit
    return biorecord.find('./BioSample').attrib['id']


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
    
    accession = convert_record(record, taxon, g)
    print(accession)
    #loaded a biosample, now put this into the SRA data check queue
    sraq.send_message(MessageBody=json.dumps(dict(biosample_id=accession,
                                                  biosample=event['biosample'],
                                                  bioproject=event.get('bioproject', ""))))
    
    return ''