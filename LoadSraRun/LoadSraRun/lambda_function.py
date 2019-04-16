import boto3
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
sra_capture_queue = os.environ.get('SRA_CAPTURE_QUEUE', 'edb-sra-capture')

if api_key:
    print(f"API key found in environment: {api_key}")
    link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=biosample&id={accession}&db=sra&api_key={api_key}"
    fetch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}&api_key={api_key}"
else:
    print("No API key found")
    link = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{tool}.fcgi?dbfrom=biosample&id={accession}&db=sra"
    fetch = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}"

sqs = boto3.resource("sqs", region_name=os.environ.get("SERVICE_REGION", "us-east-1"))
sraq = sqs.get_queue_by_name(QueueName=sra_capture_queue)

def load_record(run, experiment, biosample, uri, g):
    "Load run record"
    exp_accession = experiment.find('.//EXPERIMENT').attrib['accession']
    run_accession = run.find('./RUN').attrib['accession']
    sub_accession = experiment.find('.//SUBMISSION').attrib['accession']
    
    r = g.V()
    
    f = g.V()
         .coalesce(
         
         
         ).property('crammed', False)
          
    
    return f


def lambda_handler(event, context):
    
    g = graph.traversal().withRemote(DriverRemoteConnection(db, 'g')) ###!!!
    
    accession = event['biosample_id']
    record = xml.fromstring(
        requests.get(
            link.format(accession=accession, 
                       api_key=api_key)).text)
                       
    biosample = record.find('.//SAMPLE_DESCRIPTOR/IDENTIFIERS/EXTERNAL_ID').text                   
    
    for item in record.findall('.//Link/Id'):
        link_id = item.text
        record = xml.fromstring(
            requests.get(
                fetch.format(database='sra',
                             accession=accession, 
                             api_key=api_key)).text)
        for run in record.findall('.//RunSet/Run'):
            sra_accession = run.attrib['accession']
            if not list(g.E().hasLabel('NAMED_IN')
                         .filter(__.properties().values('name').is_(sra_accession))):
                #this one is new
                uri = f's3://edb/{biosample}/runs/{sra_accession}/'
                ebd_id = load_record(run, record, biosample, uri, g)
                sraq.send_message(MessageBody=json.dumps(dict(data=dict(accession=sra_accession,
                                                                        biosample=event.get('biosample', ''),
                                                                        bioproject=event.get('bioproject', '')
                                                                        s3Bucket=uri,
                                                                        edb_record_id = edb_id)),
                                                              results=dict()))
            
    return ''