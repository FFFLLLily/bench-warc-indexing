import warc
import os
import sys
import traceback
from bs4 import BeautifulSoup
from elasticsearch import helpers
from elasticsearch import Elasticsearch, TransportError, ElasticsearchException
import spacy
#import argparse

# Configuration
input_dir = './input' # Put .warc.gz files under ./input directory
host = 'localhost'
port = 9200
index_name = 'better_eng'
create = False

# Initialisation of ES & spaCy
es = Elasticsearch(hosts=[{"host": host, "port": port}],
                   retry_on_timeout=True, max_retries=10)
# A pre-built model is required
# python -m spacy download en_core_web_sm
nlp = spacy.load("en_core_web_sm") # Für Englisch

# Remove target index
# FOR TEST ONLY!!!
es.indices.delete(index=index_name, ignore=[400, 404])

# Settings for initialising ES index
settings = {
    'settings': {
        'index': {
            # Optimize for loading; this gets reset when we're done.
            'refresh_interval': '-1',
            'number_of_shards': '5',
            'number_of_replicas': '0',
        },
        # Set up a custom unstemmed analyzer.
        'analysis': {
            'analyzer': {
                'english_exact': {
                    'tokenizer': 'standard',
                    'filter': [
                        'lowercase'
                    ]
                }
            }
        }
    },
    'mappings': {
        # 'docs' is a type for ES 6.X
        # For ES 7.x, please annotate 'docs' field
        'docs':{
            'properties': {
                'text': {
                    # text is stemmed; text.exact is not.
                    'type': 'text',
                    'analyzer': 'english',
                    'fields': {
                        'exact': {
                            'type': 'text',
                            'analyzer': 'english_exact'
                        }
                    }
                },
                'orig': {
                    'type': 'object',
                    'enabled': False, # don't index this
                }
            }
        }
    }
}

# Create ES index
if create or not es.indices.exists(index=index_name):
    try:
        es.indices.create(index=index_name, body=settings)
    except TransportError as e:
        print(e.info)
        sys.exit(-1)

# Read warc files
def read_warc_file(file_path):
    print('Reading ' + file_path)
    with warc.open(file_path) as f:
        for record in f:
            try:
                type = record["WARC-Type"]
                if type != 'response':
                    continue
                warc_file_name = record["WARC-TREC-ID"]
                date = record["WARC-Date"]
                url = record['WARC-Target-URI']
                uid = record['WARC-RECORD-ID'].replace("<urn:uuid:", "").replace(">", "")
                content = record.payload.read()
                soup = BeautifulSoup(content, 'html.parser')
                [script.extract() for script in soup.findAll('script')]
                [style.extract() for style in soup.findAll('style')]

                if soup.body is not None:
                    content_body = soup.body.get_text()
                    text = content_body.replace("\t", " ").replace("\n", " ").replace("  ", " ")
                    # Extract & prettify text via spaCy
                    spacy_text = nlp(text)
                    text = '\n'.join([str(sent) for sent in spacy_text.sents])
                    # Extract entities via spaCy
                    pl = []
                    gl = []
                    ol = []
                    el = [] # Li: add event entities
                    for ent in spacy_text.ents:
                        if ent.label_ == 'PERSON':
                            pl.append(ent.text)
                        elif ent.label_ == 'GPE':
                            gl.append(ent.text)
                        elif ent.label_ == 'ORG':
                            ol.append(ent.text)
                        elif ent.label_ == 'EVENT':
                            el.append(ent.text)
                        else:
                            continue
                else:
                    text = ""

                if soup.title is not None:
                    title = soup.title.string
                else:
                    title = text[:30]
                  
                # Li: 'action' parameter for helpers.bulk
                data_dict = {
                    "_index":index_name,
                    "_id":uid,
                    "_type":'docs',
                }

                source_block = {
                    "uuid":uid,
                    "title":title,
                    "orig":{
                        "derived-metadata":{
                            "text":text,
                            "warc-file":warc_file_name
                        },
                        "WARC-Target-URI":url
                    },
                    "guess-publish-date":date,
                    "text":text,
                    "PERSON":pl,
                    "ORG":ol,
                    "GPE":gl,
                    "EVENT":el #Li: add event
                }
                
                data_dict['_source'] = source_block
                
            except:
                traceback.print_exc()
            yield data_dict
            
# Index
for root, dirs, files in os.walk(input_dir):
    for file_name in files:
        if(file_name.endswith('.warc.gz')):
            file_path = input_dir + '/' + file_name
            print("Indexing...")
            helpers.bulk(es, read_warc_file(file_path), request_timeout=30)

'''
es.indices.put_settings(index=index_name,
                        body={'index': { 'refresh_interval': '1s',
                                         'number_of_replicas': '1',
                        }})
'''

