import os
import re

import ner
import pycountry
import datetime
import dateutil.parser

class Diags(object):
    def __init__(self):
        self.tagger = ner.SocketNER(host='localhost', port=2020)

    def show_ner_tags(self):
        input_dir = '/Users/altaf/Projects/reuters/data'
        file_list = os.listdir(input_dir)
        for filename in sorted(file_list):
            if filename.endswith('.text'):
                self.process_one_file(os.path.join(input_dir, filename))

    def compare_file_lengths(self, dir1, dir2, transform=None):
        file_list = os.listdir(dir1)
        match = 0
        not_match = 0
        for filename in sorted(file_list):
            file1 = os.path.join(dir1, filename)
            if transform:
                filename = filename.replace(transform[0], transform[1])
            file2 = os.path.join(dir2, filename)
            if self.compare_file_length(file1, file2):
                match += 1
            else:
                not_match += 1
        print '    matches', match
        print 'not matches', not_match


    def compare_file_length(self, file1, file2):
        filelen1 = self.file_length(file1)
        filelen2 = self.file_length(file2)
        if filelen1 == filelen2:
            print file1, ':', filelen1, '==', filelen2
            return True
        else:
            print file1, ':', filelen1, '!=', filelen2
            return False

    def file_length(self, filename):
        with open(filename) as f:
            for i, l in enumerate(f, 1):
                pass
        return i

    def remove_tags(self, line, tags, tags_only):
        regex = '(' + '|'.join(['<%s>|</%s>' % (t,t) for t in tags_only] + ['<%s>.*?</%s>' % (t, t) for t in tags]) + ')'
        p = re.compile(regex)
        return p.sub('', self.tagger.tag_text(line))

    def process_one_file(self, in_filename, out_filename, date):
        with open(in_filename, 'r') as infile, open(out_filename, 'w') as outfile:
            for line in infile:
                outfile.write('[%s] %s\n' % (str(date).replace('-', '_'), self.remove_tags(line, ['LOCATION'], ['ORGANIZATION', 'PERSON'])))

    def show_entities(self, entities, c):
        if c != 'ORGANIZATION':
            return
        for e in entities[c]:
            print e

    def check_country_code(self, code):
        GBR = ['NIR', 'ENG', 'SCT', 'WLS']
        skipped = ['NONE', 'ANT']
        code = code.upper()

        if code in GBR:
            code = 'GBR'
        if code in skipped:
            return

        try:
            c = pycountry.countries.get(alpha3=code)
            print c.name.replace(' ', '-')
        except:
            print 'country not found:', code

    def check_country_codes(self):
        input_dir = '/Users/altaf/Projects/reuters/features'
        file_list = os.listdir(input_dir)
        for filename in sorted(file_list):
            with open(os.path.join(input_dir, filename), 'r') as f:
                for line in f:
                    print line
                    for i in line.split():
                        self.check_country_code(i.split('=')[0])

    def get_country_codes(self, line):
        codes = [t.split('=')[0].upper() for t in line.split()]
        if 'NONE' in codes:
            codes.remove('NONE')
        return codes

    def get_date_from_filename(self, filename):
        filename_regex = re.compile(r'^(\d{4}-\d{2}-\d{2})\..*$')
        match = filename_regex.match(filename)
        return dateutil.parser.parse(match.group(1)).date()

    def fix_features_files(self):
        input_dir = '/Users/altaf/Projects/reuters/features'
        output_dir = '/Users/altaf/Projects/reuters/features.2'

        file_list = os.listdir(input_dir)
        reference_date = self.get_date_from_filename(sorted(file_list)[0])

        for filename in sorted(file_list):
            filename_date = self.get_date_from_filename(filename)
            days_delta = (filename_date - reference_date).days
            timeval = float(days_delta) / float(len(file_list))
            infile_path = os.path.join(input_dir, filename)
            outfile_path = os.path.join(output_dir, filename)
            with open(infile_path, 'r') as infile, open(outfile_path, 'w') as outfile:
                for line in infile:
                    codes = self.get_country_codes(line)
                    clist = ['%s=1' % c for c in sorted(set(codes))]
                    #outfile.write(' '.join(clist) + '\n')
                    outfile.write('time=%f %s\n' % (timeval, ' '.join(clist)))

    def remove_ner_tags(self, input_dir, output_dir):
        file_list = os.listdir(input_dir)
        for filename in sorted(file_list):
            print 'processing', filename
            date = self.get_date_from_filename(filename)
            self.process_one_file(os.path.join(input_dir, filename), os.path.join(output_dir, filename), date)


diags = Diags()
#diags.show_ner_tags()
#diags.fix_features_files()

#diags.remove_ner_tags('/Users/altaf/Projects/reuters/data', '/Users/altaf/Projects/reuters/data.2')
diags.compare_file_lengths('/Users/altaf/Projects/reuters/data.2', '/Users/altaf/Projects/reuters/features.2', ['text', 'text.features'])
