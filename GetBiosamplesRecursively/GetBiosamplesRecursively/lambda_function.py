import boto3
import json
import requests
import os
from xml.etree import ElementTree as xml

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

max_recursion = int(os.environ.get('MAX_RECURSIVE_DEPTH', '4'))

db = os.environ.get('EDB_DB', "wss://biosurvdbtest.cvkyaz9id4ml.us-east-1.neptune.amazonaws.com:8182/gremlin")
bp_queue = os.environ.get('BIOPROJECT_QUEUE', "edb-bioprojects")
bs_queue = os.environ.get('BIOSAMPLE_QUEUE', 'edb-biosamples')
api_key = os.environ.get('NCBI_API_KEY', "")

if api_key:
    print(f"API key found in environment: {api_key}")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}&api_key={api_key}"
else:
    print("No API key found")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}"

sqs = boto3.resource("sqs", region_name=os.environ.get("SERVICE_REGION", "us-east-1"))
#print("sqs")
bpq = sqs.get_queue_by_name(QueueName=bp_queue)
#print("bpq")
bsq = sqs.get_queue_by_name(QueueName=bs_queue)
#print("bsq")


def lambda_handler(event, context):
    """Get Biosamples that are part of the project and add them to the BioSample
       processing queue. Then add sub-projects to the BioProject queue."""
    print(event['bioproject'])
    recursive_depth = event.get('recursive_depth', 0)
    print(f"recursive depth {recursive_depth}")
    recursive_depth += 1
    record = xml.fromstring(
        requests.get(
            req.format(database="bioproject", 
                       accession=event['bioproject'], 
                       api_key=api_key)).text)
    for tag in record.findall('.//LocusTagPrefix'):
        try:
            biosample = tag.attrib['biosample_id']
            print(biosample)
        except KeyError:
            pass
        else:
            # bs_record = xml.fromstring(
            #     requests.get(req.format(database="biosample",
            #                             accession=biosample,
            #                             api_key=api_key)).text)
            # biosample = bs_record.find('''.//Id[@is_primary="1"]''')
            # bs_dict = {a.attrib['attribute_name']:a.text for a in bs_record.findall('.//Attribute')}
            graph = Graph()
            g = graph.traversal().withRemote(DriverRemoteConnection(db, 'g')) ###!!!
            if not list(g.E().has('NAMED_IN', 'name', biosample)): 
                #add the biosample to the queue
                bsq.send_message(MessageBody=json.dumps(dict(biosample=biosample)))
    if recursive_depth < max_recursion:
        #get all of the child bioprojects and dump them in the queue
        for link in (dict(recursive_depth=recursive_depth,
                      bioproject=link.attrib['accession']) 
                      for link in record.findall(".//ProjectLinks/Link/ProjectIDRef")
                      if link.attrib['accession'].upper() != event['bioproject'].upper()):
            bpq.send_message(MessageBody=json.dumps(link))