import os
import ner
import time
import socket

import geocoder
import dstk
import pickle

from geopy.point import Point

from geopy.geocoders import GeoNames
from geopy.geocoders import GoogleV3
from geopy.geocoders import Nominatim
from geopy.geocoders import GeocodeFarm

from geopy.exc import \
    GeocoderTimedOut, \
    GeocoderServiceError, \
    GeocoderAuthenticationFailure, \
    GeocoderQueryError, \
    GeocoderInsufficientPrivileges

class Location(object):
    def __init__(self, point, country):
        self.point = point
        self.country = country

class NamedEntityMap(object):
    def __init__(self):
        self.filename = '/Users/altaf/Projects/reuters/ner_db.dat'
        self.db = dict()
        self.load()

    def load(self):
        if (os.path.isfile(self.filename)):
            print 'loading db'
            with open(self.filename, 'r') as f:
                self.db = pickle.load(f)
        print 'number of records in db:', len(self.db)

    def save(self):
        print 'saving db'
        with open(self.filename, 'wb') as f:
            pickle.dump(self.db, f)
        print 'number of records in db:', len(self.db)

    def lookup(self, entity):
        if entity in self.db:
            location = self.db[entity]
            return Location(Point(location[0], location[1]), location[2])
        else:
            return None

    def add(self, entity, point, country):
        self.db[entity] = (point.latitude, point.longitude, country)
        return Location(point, country)

class NamedEntityTagger(object):
    def __init__(self):
        self.db = NamedEntityMap()
        self.tagger = ner.SocketNER(host='localhost', port=2020, output_format='slashTags')
        #self.geolocator = GeoNames(username='altaf.ali')
        #self.geolocator = GoogleV3()
        self.geolocator = GeocodeFarm(api_key='3ae1656ecc66581156dc23e6bf7a182d1fccdf49')
        self.dstk = dstk.DSTK({'apiBase':'http://localhost:8085'})

    def lookup(self, entity):
        location = self.db.lookup(entity)
        if location:
            return location

        location = self.geolocate(entity)
        if location == None:
            return None

        address = location.raw['ADDRESS']
        print '%s -> %s' % (entity, address['address_returned'])
        coordinates = location.raw['COORDINATES']
        point = Point(coordinates['latitude'], coordinates['longitude'])
        politics = self.dstk.coordinates2politics((point.latitude, point.longitude))[0]['politics']
        country = None
        if politics:
            for c in politics:
                if c['type'] == 'admin2':
                    country = c['code']

        if country == None:
            return None

        return self.db.add(entity, point, country)

    def geolocate(self, entity):
        timeout = 1
        for i in range(30):
            try:
                location = self.geolocator.geocode(entity)
                return location
            except (GeocoderServiceError, GeocoderAuthenticationFailure, GeocoderQueryError, GeocoderInsufficientPrivileges):
                return None

            except (socket.timeout, GeocoderTimedOut):
                print 'received timeout, retrying in %d seconds', timeout
                time.sleep(timeout)
                timeout = timeout * 2

    def tag_file(self, in_filename, out_filename):
        if os.path.isfile(out_filename):
            print 'skipping', in_filename
            return

        print 'creating features file:', in_filename, '->', out_filename
        outfile_data = []
        with open(in_filename, 'r') as infile:
            for line in infile:
                entities = self.tagger.get_entities(line)
                countries = []
                if 'LOCATION' in entities:
                    entities = set([s.lower() for s in entities['LOCATION']])
                    for e in entities:
                        location = self.lookup(e)
                        if location == None:
                            continue
                        countries.append(location.country)

                clist = ['%s=1' % c for c in sorted(set(countries))]
                outfile_data.append(' '.join(clist))

        with open(out_filename, 'w') as outfile:
            for line in outfile_data:
                outfile.write(line + '\n')

        # save the db
        self.db.save()

tagger = NamedEntityTagger()

input_dir = '/Users/altaf/Projects/reuters/data'
file_list = os.listdir(input_dir)
for filename in sorted(file_list):
    if filename.endswith('.features'):
        continue
    path = os.path.join(input_dir, filename)
    tagger.tag_file(path, path + '.features')
