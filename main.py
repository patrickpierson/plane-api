from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import exceptions
import glob
import json

ELASTICSEARCH_URL = 'localhost:9200'
es = Elasticsearch()


def get_data():
    '''
    Gets a list of files from data directory
    :return: list
    '''
    data_files = []
    for i in glob.iglob('data/**/*.json', recursive=True):
        data_files.append(i)
    return data_files


def get_aircraft(file_loc):
    '''
    Gets a file location and returns data in json form
    :param file_loc:
    :return: json
    '''
    with open(file_loc, 'r') as data:
        json_data = json.loads(data.read())
        json_data['file_name'] = file_loc
        return json_data


def parse_aircrafts(aircrafts):
    '''
    Gets a dict of aircrafts and now datetime and returns formatted list of aircraft and datetime
    :param aircrafts:
    :return: list
    '''
    aircrafts_list = []
    for i in aircrafts['aircraft']:
        i['time_seen'] = datetime.utcfromtimestamp(aircrafts['now'])
        i['file_name'] = aircrafts['file_name']
        i['epoch'] = aircrafts['now']
        if 'alt_baro' in i:
            i['alt_baro'] = str(i['alt_baro'])
        aircrafts_list.append(i)
    return aircrafts_list


def send_elasticsearch(aircrafts_list):
    for i in aircrafts_list:
        print('Sending %s to Elasticsearch' % i['hex'])
        entry_uuid = i['hex'] + '-' + i['time_seen'].strftime('%s')
        try:
            es.index(index='test', doc_type='aircraft', id=entry_uuid, body=i)
        except exceptions.RequestError as e:
            print(e)

#Todo: https://github.com/vinsci/geohash

def main():
    pass


if __name__ == "__main__":
    for i in get_data():
        aircrafts = get_aircraft(i)
        aircrafts_list = parse_aircrafts(aircrafts)
        send_elasticsearch(aircrafts_list)
    # main()
