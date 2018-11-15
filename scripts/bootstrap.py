#!/usr/bin/env python2.7

import codecs
import datetime
import math
import os
import random
import sys

from couchdb import client
from couchdb import design
from gpretty.command_utils.prettify import ColourMixin


def _rand(low, high):
    return int(math.floor(random.random() * (high - low) + low))


def _write_trans(s):
    """Write a 'transient' string which the next line will overwrite"""
    sys.stdout.write(' ' * 50 + '\r')
    sys.stdout.write('%s\r' % s)
    sys.stdout.flush()


class ViewReader(object):
    def __init__(self, path='views'):
        self.view_path = path

    def _read_view(self, doc_name, view_name):
        """
        Read a view corresponding to the given doc_name and view_name

        Views must be stored under "views/" in a format like
            views/{design doc}/{view name}/map.js + reduce.js

        Reduce is optional

        :param doc_name: The design document the view will be written to
        :param view_name: The name of the view

        :returns: The constructed couchdb view definition
        :rtype: couchdb.design.ViewDefinition
        """
        map = None
        reduce = None

        path = os.path.join(self.view_path, doc_name, view_name)
        map_path = os.path.join(path, 'map.js')
        reduce_path = os.path.join(path, 'reduce.js')

        with open(map_path, 'rb') as f:
            map = f.read()

        if os.path.isfile(reduce_path):
            with open(reduce_path, 'rb') as f:
                reduce = f.read()
        else:
            reduce = None

        return design.ViewDefinition(doc_name, view_name, map, reduce)

    def read_views(self):
        """
        Read all views in the views/ dir and create ViewDefinition instances

        :rtype: list(ViewDefinition)
        """
        views = []

        for design_doc in sorted(os.listdir(self.view_path)):
            doc_path = os.path.join(self.view_path, design_doc)
            if not os.path.isdir(doc_path):
                continue

            for view in sorted(os.listdir(doc_path)):
                view_path = os.path.join(doc_path, view)
                if not os.path.isdir(view_path):
                    continue

                views.append(self._read_view(design_doc, view))

        return views


class Bootstrapper(object):
    def __init__(self):
        with codecs.open('fixtures/forenames.txt', 'rb', encoding='utf8') as f:
            self.forenames = [l.strip() for l in f.readlines()]

        with codecs.open('fixtures/surnames.txt', 'rb', encoding='utf8') as f:
            self.surnames = [l.strip() for l in f.readlines()]

        with codecs.open(
            'fixtures/occupations.txt', 'rb', encoding='utf8'
        ) as f:
            self.occupations = [l.strip() for l in f.readlines()]

        with codecs.open('fixtures/races.txt', 'rb', encoding='utf8') as f:
            self.races = [l.strip() for l in f.readlines()]

        with codecs.open('fixtures/places.txt', 'rb', encoding='utf8') as f:
            self.places = [l.strip() for l in f.readlines()]

    def generate_person(self):
        """Generate a random person"""
        forename = random.choice(self.forenames)
        surname = random.choice(self.surnames)
        name = '%s %s' % (forename, surname)
        occupation = random.choice(self.occupations)
        race = random.choice(self.races)
        birthplace = random.choice(self.places)

        return {
            'type': 'customer',
            'name': name,
            'age': _rand(4, 80),
            'occupation': occupation,
            'height': _rand(100, 230),
            'weight': _rand(40, 140),
            'family': surname,
            'birthplace': birthplace,
            'email': '%s.%s@gmail.com' % (forename.lower(), surname.lower()),
            'race': race
        }

    def generate_visit(self, cust_id):
        ts = datetime.datetime(
            _rand(2000, 2018), _rand(1, 12), _rand(1, 28),
            _rand(0, 23), _rand(0, 59), _rand(0, 59)
        )
        return {
            'type': 'visit',
            'customer': cust_id,
            'timestamp': ts.strftime('%Y-%m-%d %H:%M:%S'),
            'ip_address': '.'.join([str(_rand(1, 255)) for _ in xrange(4)])
        }


class Command(ColourMixin):
    def __init__(self):
        self.couch = client.Server()
        self.db = self.couch['customer']
        self.bs = Bootstrapper()

    def recreate_db(self):
        print self.colourise('Dropping and recreating db...')

        del self.couch['customer']
        self.couch.create('customer')
        self.db = self.couch['customer']

    def write_views(self, views_dir='views'):
        print self.colourise('Writing views to db...')

        reader = ViewReader(views_dir)

        for v in reader.read_views():
            print '  - %s/%s' % (v.design, v.name)
            v.sync(self.db)

    def generate_visits(self):
        """
        Generate very arbitrary visits based on criteria determined by my
        whims
        """
        print self.colourise('Generating visit data...')

        visits = []

        print self.colourise('  - Users over 16...', 'cyan')
        for r in self.db.view(
            'tests/customer_names_by_age', reduce=False
        )[16:]:
            visits.append(self.bs.generate_visit(r.id))
            _write_trans('    - %s' % r.value)

        print ''
        print self.colourise('  - Users 40-80kg...', 'cyan')
        for r in self.db.view(
            'tests/customer_heights_by_weight', include_docs=True, reduce=False
        )[40:80]:
            visits.append(self.bs.generate_visit(r.id))
            _write_trans('    - %s' % r.doc['name'])

        print ''
        print self.colourise('Bulk inserting %d visits...' % len(visits))
        self.db.update(visits)

    def generate_fixtures(self, n=10):
        print self.colourise('Bootstrapping %d customer records...' % n)

        for _ in xrange(0, n):
            p = self.bs.generate_person()
            _write_trans('  - %s' % p['name'])
            self.db.save(p)

        print ''

    def write_observations(self):
        """
        Generate some random observations from our generated data
        """
        williams = list(
            self.db.view(
                'tests/customers_by_family', reduce=True, group=True, limit=1
            )['Williams']
        )[0].value

        kids = list(
            self.db.view(
                'tests/customer_names_by_age', reduce=True, group=False,
                inclusive_end=True, limit=1
            )[:17]
        )[0].value

        visits_2016 = list(
            self.db.view(
                'tests/visits_by_timestamp_by_ip', reduce=True, group=False,
                inclusive_end=False, limit=1
            )[['2016-01-01 00:00:00', {}]:['2017-01-01 00:00:00', None]]
        )[0].value

        last_visitor = list(
            self.db.view(
                'tests/visits_by_timestamp_by_ip', descending=True,
                reduce=False, include_docs=True, limit=1
            )
        )[0]
        last_visitor = (
            last_visitor.doc['name'],
            last_visitor.key[1]
        )

        print self.colourise('Observations for our generated data:')
        print '  - %d customers have the surname "Williams"' % williams
        print '  - %d customers are children' % kids
        print '  - %d visits occurred in 2016' % kids
        print '  - The latest visit is from %s (%s)' % last_visitor

    def run(self, *args, **kwargs):
        self.recreate_db()
        self.write_views()
        self.generate_fixtures(500)
        self.generate_visits()

        print ''
        self.write_observations()

        print ''
        print self.colourise('Done!', 'green')


if __name__ == '__main__':
    Command().run(sys.argv[1:])
