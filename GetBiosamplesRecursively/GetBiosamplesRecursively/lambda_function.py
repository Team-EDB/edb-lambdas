import requests
import os
from xml.etree import ElementTree as xml

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

max_recursion = int(os.environ.get('MAX_RECURSIVE_DEPTH', '4'))

db = os.environ.get('EDB_DB', "")

api_key = os.environ.get('NCBI_API_KEY', "")
if api_key:
    print("API key found in environment")
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}&api_key={api_key}"
else:
    req = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db={database}&id={accession}"


def lambda_handler(event, context):
    recursive_depth = event.get('recursive_depth', 0)
    print(f"depth {recursive_depth}")
    recursive_depth += 1
    record = xml.fromstring(
        requests.get(
            req.format(database="bioproject", 
                       accession=event['bioproject'], 
                       api_key=api_key)).text)
    for tag in record.findall('.//LocusTagPrefix'):
        try:
            biosample = tag.attrib['biosample_id']
            bs_record = xml.fromstring(
                requests.get(req.format(database="biosample",
                                        accession=biosample,
                                        api_key=api_key)).text)
            load_new_biosample(bs_record)
        except KeyError:
            pass
    if recursive_depth < max_recursion:
        links = [dict(recursive_depth=recursive_depth,
                      bioproject=link.attrib['accession']) for link in record.findall(".//ProjectLinks/Link/ProjectIDRef")]
        return links
        
        
def load_new_biosample(bs_record, graph=Graph()):
    biosample = bs_record.find('''.//Id[@is_primary="1"]''')
    bs_dict = {a.attrib['attribute_name']:a.text for a in bs_record.findall('.//Attribute')}
    print(bs_dict)
    
    g = graph.traversal().withRemote(DriverRemoteConnection(db,'edb'))