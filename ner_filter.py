import os
import re
import operator

import pickle
import tempfile
import collections

import ner
import nltk

import pycountry
import geonamescache

from nltk.stem.porter import PorterStemmer

input_dir = '/Users/altaf/Projects/reuters/data.full'
output_dir = '/Users/altaf/Projects/GibbsAT++/compare'
output_file = os.path.join(output_dir, 'new/data.txt')
output_file_at = os.path.join(output_dir, 'at/data.txt')
output_file_at1 = os.path.join(output_dir, 'at1/data.txt')
output_file_lda = os.path.join(output_dir, 'lda/data.txt')
vocab_file = os.path.join(output_dir, 'wordmap.dat')

tagger = ner.SocketNER(host='localhost', port=2020, output_format='slashTags')

class NE_Filter(object):
    def __init__(self):
        self.vocab = {}
        self.tagger = ner.SocketNER(host='localhost', port=2020)
        self.geonames = geonamescache.GeonamesCache()
        self.countries = self.geonames.get_countries_by_names()
        self.re_actor = re.compile(r'<(PERSON|ORGANIZATION)>(.*?)</(PERSON|ORGANIZATION)>')
        self.re_location = re.compile(r'<LOCATION>(.*?)</LOCATION>')
        #self.re_tags = re.compile(r'<.*?>.*?<.*?>')
        self.re_tokens = re.compile(r'\w+(?:\'\w+)?')
        self.re_parens = re.compile(r'\(.*?([^\)]+)\)')
        self.stopword_list = [ item.strip() for item in open("stopwords.txt").readlines() ]
        self.recover_list = {"wa":"was", "ha":"has"}
        self.lemmatizer = nltk.WordNetLemmatizer()
        self.stemmer = PorterStemmer()
        self.ignored_actors = ['reuters', 'new_york_times', 'washington_post']
        # debugging
        self.reset_debug_stats()

    def reset_debug_stats(self):
        self.total_locations = 0
        self.matched_locations = 0

    def print_debug_stats(self):
        if self.matched_locations < self.total_locations:
            print '      locations matched:', self.matched_locations, '/', self.total_locations

    def is_stopword(self, w):
        return w in self.stopword_list

    def lemmatize(self, w0):
        w = self.lemmatizer.lemmatize(w0.lower())
        #w = self.stemmer.stem(w0.lower())

        #if w=='de': print w0, w
        if w in self.recover_list:
            return self.recover_list[w]
        return w

    def tokenize(self, text):
        return self.re_tokens.findall(text)

    def tokenize_and_lemmatize(self, text):
        tokens = self.tokenize(text)
        terms = []
        for t in tokens:
            term = self.lemmatize(t)
            if not re.match(r'[a-z\.\-]+$', term):
                continue
            if self.is_stopword(term) or len(term)<3:
                continue
            terms.append(term)
        return terms

    def remove_last_parenthesis(self, text):
        start = 0
        end = 0
        result = ''
        for m in self.re_parens.finditer(text):
            skipped_tokens = ['reporting', 'writing', 'editing']
            match = m.group(0).lower()
            if any(x in match for x in skipped_tokens):
                match = ''
            end = m.start()
            result += text[start:end] + match
            start = m.end()
        result += text[start:]
        return result.strip()

    def get_named_entities(self, regex, tagged_text):
        return [s.lower() for s in regex.findall(tagged_text)]

    def parse_actors(self, text, remove_actors=True):
        tagged_text = self.tagger.tag_text(text)

        text = ''
        actors = []
        start = 0
        end = 0
        for m in self.re_actor.finditer(tagged_text):
            tag = ''
            if remove_actors:
                actor = '_'.join(self.tokenize(m.group(2))).lower()
                if (actor not in self.ignored_actors):
                    actors.append(actor)
            else:
                tag = m.group(2)
            end = m.start()
            text += tagged_text[start:end] + tag + ' '
            start = m.end()
        text += tagged_text[start:]
        return (actors, text)

    def parse_locations(self, tagged_text):
        locations = []
        text = ''
        start = 0
        end = 0
        for m in self.re_location.finditer(tagged_text):
            self.total_locations += 1
            tag = m.group(1)
            country = self.find_country(tag)
            if country:
                tag = ' '
                locations.append(country)
                self.matched_locations += 1

            end = m.start()
            text += tagged_text[start:end] + tag
            start = m.end()
        text += tagged_text[start:]
        return (locations, text)

    def parse_one_doc(self, doc, remove_actors=True):
        text = self.remove_last_parenthesis(doc)
        (actors, text) = self.parse_actors(text, remove_actors)
        (locations, text) = self.parse_locations(text)
        text = self.tokenize_and_lemmatize(text)
        return (collections.Counter(locations), list(set(actors)), text)

    def make_vocab(self, input_dir, vocab_file):
        for filename in os.listdir(input_dir):
            print 'pre-processing file', filename
            with open(os.path.join(input_dir, filename), 'r') as f:
                for line in f:
                    text = self.remove_last_parenthesis(line)
                    (actors, text) = self.parse_actors(text)
                    for actor in actors:
                        if actor in self.vocab:
                            self.vocab[actor] += 1
                        else:
                            self.vocab[actor] = 1

        pickle.dump(self.vocab, open(vocab_file, 'w'))

    def run(self, input_dir, output_file, output_file_lda, output_file_at, output_file_at1, vocab_file):
        count = 0
        nfiles = 0
        max_files = 1000
        max_lines = 1000
        self.make_vocab(input_dir, vocab_file)

        #self.wordmap = pickle.load(open(vocab_file))

        print 'writing file', output_file
        with tempfile.TemporaryFile() as tempf, \
                open(output_file, 'w') as outfile, \
                open(output_file_lda, 'w') as outfile_lda, \
                open(output_file_at, 'w') as outfile_at, \
                open(output_file_at1, 'w') as outfile_at1:
            for filename in os.listdir(input_dir):
                #if filename != '2010-10-26.text':
                #    continue
                print 'processing file', filename
                infilename = os.path.join(input_dir, filename)
                with open(infilename, 'r') as infile:
                    nlines = 0
                    for line in infile:
                        (locations, actors, text) = self.parse_one_doc(line)
                        #print len(text), len(theta), len(locations), theta
                        actors = [x for x in actors if x in self.vocab and self.vocab[x] > 10]
                        #print filename.split('.')[0] + ';'  + ','.join(locations) + ';' + ','.join(theta) + ';' + ' '.join(text)
                        nlines += 1

                        if nlines >= max_lines:
                            break

                        if (len(locations) > 0 and len(actors) > 0 and len(text) > 0):
                            tempf.write(filename.split('.')[0] + ';'  + ','.join(locations) + ';' + ','.join(actors) + ';' + ' '.join(text) + '\n')
                            count += 1
                nfiles += 1
                if nfiles >= max_files:
                    break

            tempf.seek(0)
            outfile.write(str(count) + '\n')
            outfile_lda.write(str(count) + '\n')
            outfile_at.write(str(count) + '\n')
            outfile_at1.write(str(count) + '\n')
            for line in tempf:
                fields = line.split(';')
                outfile.write(line)
                outfile_lda.write(fields[3])
                outfile_at.write(fields[1] + ';' + fields[3])
                actors = fields[1].split(',')
                outfile_at1.write(actors[0] + ';' + fields[3])

    def find_country_in_pycountry(self, search_term):
        search_term = search_term.replace('.','')
        fields = ['name', 'alpha2', 'alpha3', 'official_name'] #'numeric_code', 'common_name'
        for f in fields:
            try:
                country = pycountry.countries.get(**{f : search_term})
                return country.alpha3
            except:
                pass

        return None

    def find_country_in_geonames(self, search_term):
        if search_term in self.countries:
            return self.countries[search_term]['iso3']

        cities = self.geonames.get_cities_by_name(search_term)
        if len(cities):
            for city in cities:
                for v in city.values():
                    # print '     ', v['name'], v['countrycode']
                    if 'countrycode' in v:
                        return self.find_country_in_pycountry(v['countrycode'])

        return None

    def find_country(self, search_term):
        for t in [search_term, search_term.title(), search_term.capitalize()]:
            country = self.find_country_in_pycountry(t)
            if country:
                return country

            country = self.find_country_in_geonames(t)
            if country:
                return country

        return None


    def make_dmr_corpus(self, input_dir, output_dir):
        nfiles = 0
        max_files = 10000
        max_lines = 10000

        print 'output dir', output_dir
        for filename in os.listdir(input_dir):
            self.reset_debug_stats()
            print 'processing file', filename
            infilename = os.path.join(input_dir, filename)

            outfilename_data = os.path.join(output_dir, 'data', filename)
            outfilename_featuers = os.path.join(output_dir, 'features', filename)

            with open(infilename, 'r') as infile, \
                    open(outfilename_data, 'w') as outfile_data, \
                    open(outfilename_featuers, 'w') as outfile_features:
                nlines = 0
                for line in infile:
                    (locations, actors, text) = self.parse_one_doc(line, remove_actors=False)
                    nlines += 1

                    if (len(locations) > 0 and len(text) > 0):
                        outfile_data.write(' '.join(text) + '\n')
                        features = ['%s=%d' % (k, v) for k,v in locations.iteritems()]
                        outfile_features.write(' '.join(features) + '\n')

                    if nlines >= max_lines:
                        break

            self.print_debug_stats()

            nfiles += 1
            if nfiles >= max_files:
                break

ne_filter = NE_Filter()
#ne_filter.make_vocab(input_dir, vocab_file)
ne_filter.make_dmr_corpus(input_dir, '/Users/altaf/Projects/reuters/dmr.full')

#ne_filter.run(input_dir, output_file, output_file_lda, output_file_at, output_file_at1, vocab_file)