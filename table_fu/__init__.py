#! /usr/bin/env python
"""
This is a Python version of Propublica's TableFu Ruby library.

TableFu parses, sorts and formats table-like streams, like CSVs,
and outputs tables in HTML. It's meant as a utility to make 
getting tabular data on the web easier.
"""
from __future__ import with_statement

__version__ = "0.4.2"
__author__ = "Chris Amico (eyeseast@gmail.com)"

import csv
import urllib2
from copy import copy
from functools import total_ordering

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

try:
    import json
    has_json = True
except ImportError:
    try:
        import simplejson as json
        has_json = True
    except ImportError:
        has_json = False

from table_fu.formatting import format

class TableFu(object):
    """
    A table, to be manipulated like a spreadsheet.
    
    TableFu reads in an open CSV file, parsing it 
    into a table property, Row and Datum objects.
    
    Usage:
    
    # test.csv
    
    Author,Best Book,Number of Pages,Style
    Samuel Beckett,Malone Muert,120,Modernism
    James Joyce,Ulysses,644,Modernism
    Nicholson Baker,Mezannine,150,Minimalism
    Vladimir Sorokin,The Queue,263,Satire
    
    >>> spreadsheet = TableFu(open('../tests/test.csv'))
    >>> len(spreadsheet.rows)
    4
    >>> spreadsheet.columns
    ['Author', 'Best Book', 'Number of Pages', 'Style']
    >>> spreadsheet.columns = ['Style', 'Author']
    >>> spreadsheet.columms
    ['Style', 'Author']
    
    """
    def __init__(self, table, **options):
        """
        Takes a table argument and optional keyword arguments.

        The 'table' argument should be a two-dimensional array,
        either a list or tuple, or an open file that can be
        parsed by Python's csv module (using csv.reader)
        """
        if hasattr(table, 'next'): # for file-like objects
            csv_options = {}
            if 'dialect' in options:
                csv_options['dialect'] = options.pop('dialect')
            reader = csv.reader(table, **csv_options)
            self.table = [row for row in reader]
        else:
            self.table = table
        self.default_columns = self.table.pop(0)
        self._columns = options.get('columns', [])
        self.deleted_rows = []
        self.faceted_on = None
        self.totals = {}
        self.formatting = options.get('formatting', {})
        self.style = options.get('style', {})
        self.options = options
        if options.has_key('sorted_by'):
            col = options['sorted_by'].keys()[0]
            self.sort(column_name=col, 
            reverse=options['sorted_by'][col].get('reverse', False))

    def __getitem__(self, row_num):
        """
        Return one row in the table
        """
        return Row(self.table[row_num], row_num, self)
    
    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(list(self.table))

    def add_rows(self, *rows):
        for row in rows:
            self.table.append(row)
    
    def count(self):
        return len(list(self))
    
    @property
    def rows(self):
        return (Row(row, i, self) for i, row in enumerate(self.table))

    @property
    def headers(self):
        if self._columns:
            col_set = self._columns
        else:
            col_set = self.default_columns
        return [Header(col, i, self) for i, col in enumerate(col_set)]

    def _get_columns(self):
        if self._columns:
            return self._columns
        return self.default_columns

    def _set_columns(self, columns):
        self._columns = self.options['columns'] = list(columns)

    columns = property(_get_columns, _set_columns)

    def delete_row(self, row_num):
        self.deleted_rows.append(self.table.rows.pop(row_num))
    
    def sort(self, column_name=None, reverse=False):
        """
        Sort rows in this table, preserving a record of how that
        sorting is done in TableFu.options['sorted_by']
        """
        if not column_name and self.options.has_key('sorted_by'):
            column_name = self.options['sorted_by'].keys()[0]
        if column_name not in self.default_columns:
            raise ValueError(u"{0} isn't a column in this table".format(column_name))
        index = self.default_columns.index(column_name)
        self.table.sort(key = lambda row: row[index], reverse=reverse)
        self.options['sorted_by'] = {column_name: {'reverse': reverse}}

    def transform(self, column_name, func):
        if column_name not in self.default_columns:
            raise ValueError(u"{0} isn't a column in this table".format(column_name))

        if not callable(func):
            raise TypeError(u"{0} isn't callable".format(func))

        index = self.default_columns.index(column_name)
        for row in self.table:
            val = row[index]
            val = func(val)
            row[index] = val

    def values(self, column_name, unique=False):
        if column_name not in self.default_columns:
            raise ValueError(u"{0} isn't a column in this table".format(column_name))
        index = self.default_columns.index(column_name)
        result = [row[index] for row in self.table]
        if unique:
            return set(result)
        return result
    
    def total(self, column_name):
        if column_name not in self.default_columns:
            raise ValueError(u"{0} isn't a column in this table".format(column_name))
        
        try:
            values = (float(v) for v in self.values(column_name))
        except ValueError:
            raise ValueError(u'Column {0} contains non-numeric values'.format(column_name))
        
        return sum(values)
    
    def filter(self, func=None, **query):
        """
        Tables can be filtered in one of two ways:
         - Simple keyword arguments return rows where values match *exactly*
         - Pass in a function and return rows where that function evaluates to True
        
        In either case, a new TableFu instance is returned
        """
        if callable(func):
            result = filter(func, self)
            result.insert(0, self.default_columns)
            return TableFu(result, **self.options)
        else:
            result = self
            for column, value in query.items():
                result = result.filter(lambda r: r[column] == value)
            return result

    def facet_by(self, column):
        """
        Faceting creates new TableFu instances with rows matching
        each possible value.
        """
        faceted_spreadsheets = {}
        for row in self.rows:
            if row[column]:
                col = row[column].value
                if faceted_spreadsheets.has_key(col):
                    faceted_spreadsheets[col].append(row.cells)
                else:
                    faceted_spreadsheets[col] = []
                    faceted_spreadsheets[col].append(row.cells)

        # create a new TableFu instance for each facet
        tables = []
        for k, v in faceted_spreadsheets.items():
            v.insert(0, self.default_columns)
            table = TableFu(v)
            table.faceted_on = k
            table.formatting = self.formatting
            table.options = self.options
            tables.append(table)

        tables.sort(key=lambda t: t.faceted_on)
        return tables
    
    def transpose(self):
        table = copy(self.table)
        table.insert(0, self.default_columns)
        result = [
            [row[i] for row in table]
            for i in xrange(len(table[0]))
        ]
        
        options = self.options.copy()
        options.pop('columns', None)
        return TableFu(result, **self.options)
    
    def map(self, func, *columns):
        """
        Map a function to rows, or to given columns
        """
        if not columns:
            return map(func, self.rows)
        else:
            values = (self.values(column) for column in columns)
            result = [map(func, v) for v in values]
            if len(columns) == 1:
                return result[0]
            else:
                return result
    
    # export methods
    def html(self):
        table = u'<table>\n{0}\n{1}\n</table>'
        thead = u'<thead>\n<tr>{0}</tr>\n</thead>'.format(u''.join([u'<th>{0}</th>'.format(col) for col in self.columns]))
        tbody = u'<tbody>\n{0}\n</tbody>'.format(u'\n'.join([row.as_tr() for row in self.rows]))
        return table.format(thead, tbody)
    
    def csv(self, **kwargs):
        """
        Export this table as a CSV
        """
        out = StringIO()
        writer = csv.DictWriter(out, self.columns, **kwargs)
        writer.writerow(dict(zip(self.columns, self.columns)))
        writer.writerows(dict(row.items()) for row in self.rows)
        
        return out
    
    def dict(self):
        return (dict(row.items()) for row in self.rows)
    
    def json(self, **kwargs):
        if not has_json:
            raise ValueError(u"Couldn't find a JSON library")
        return json.dumps(list(self.dict()), **kwargs)
    
    # static methods for loading data
    @staticmethod
    def from_file(fn, **options):
        """
        Creates a new TableFu instance from a file or path
        """
        if hasattr(fn, 'read'):
            return TableFu(fn, **options)
        with open(fn) as f:
            return TableFu(f, **options)
    
    @staticmethod
    def from_url(url, **options):
        """
        Downloads the contents of a given URL and loads it
        into a new TableFu instance
        """
        resp = urllib2.urlopen(url)
        return TableFu(resp, **options)


class Row(object):
    """
    A row in a table

    Rows act like dictionaries, but look more like lists.
    Calling row['column'] returns a column lookup based on
    the default set of columns.
    """
    def __init__(self, cells, row_num, table):
        self.table = table
        self.row_num = row_num
        self.cells = list(cells)

    def __eq__(self, other):
        if not type(other) == type(self):
            return False
        return self.cells == other.cells

    def __len__(self):
        return len(self.cells)
    
    def update(self, d):
        "Update multiple cell values in place"
        for k, v in d.items():
            self[k] = v
    
    def get(self, column_name, default=None):
        """
        Return the Datum for column_name, or default.
        """
        if column_name in self.table.default_columns:
            index = self.table.default_columns.index(column_name)
            return Datum(self.cells[index], self.row_num, column_name, self.table)
        return default
    
    def keys(self):
        return self.table.columns
    
    def values(self):
        return [d.value for d in self.data]
    
    def items(self):
        return zip(self.keys(), self.values())

    def __getitem__(self, column_name):
        "Get the value for a given cell, or raise KeyError if the column doesn't exist"
        datum = self.get(column_name)
        if datum is None:
            raise KeyError(u"{0} isn't a column in this table".format(column_name))
        else:
            return datum
    
    def __setitem__(self, column_name, value):
        """
        Set the value for a given cell
        """
        if not column_name in self.table.default_columns:
            raise KeyError(u"{0} isn't a column in this table".format(column_name))
        index = self.table.default_columns.index(column_name)
        self.cells[index] = value
    
    def __iter__(self):
        """
        Iterate over values, *not keys*. Keys are accessible
        as Row.table.columns or Row.keys()
        """
        return iter(self.values())
    
    def __repr__(self):
        return u"<{0}: {1}>".format(self.__class__.__name__, self)

    def __unicode__(self):
        return u', '.join(unicode(self[column]) for column in self.table.columns)
    
    def as_tr(self):
        cells = ''.join(d.as_td() for d in self.data)
        return u'<tr id="row{row_num}" class="row {odd_even}">{cells}</tr>'.format(
            row_num=self.row_num, odd_even=odd_even(self.row_num), cells=cells)
    
    @property
    def data(self):
        return [self[col] for col in self.table.columns]


class Datum(object):
    """
    A piece of data, with a table, row and column
    """
    def __init__(self, value, row_num, column_name, table):
        self.value = value
        self.row_num = row_num
        self.column_name = column_name
        self.table = table

    def __repr__(self):
        return u"<{0}: {1}>".format(self.column_name, self.value)
        
    def __unicode__(self):
        """
        Calling str(datum) should check first for a formatted
        version of value, then fall back to the default value
        if there's no set formatting.
        """
        if self.table.formatting.has_key(self.column_name):
            func = self.table.formatting[self.column_name].get('filter', None)
            args = self.table.formatting[self.column_name].get('args', [])
            kwargs = self.table.formatting[self.column_name].get('options', {})
            if func:
                row = self.table[self.row_num]
                args = [row[arg].value for arg in args]
                return unicode(format(self.value, func, *args, **kwargs))
        return unicode(self.value)
    
    def __eq__(self, other):
        if type(other) == type(self):
            return self.value == other.value
        else:
            return self.value == other
    
    def __lt__(self, other):
        if type(other) == type(self):
            return self.value < other.value
        else:
            return self.value < other
    
    def as_td(self):
        return u'<td style="{style}" class="datum">{value}</td>'.format(style=self.style or '', value=self)
    
    def _get_style(self):
        try:
            return self.table.style[self.column_name]
        except KeyError:
            return None
    style = property(_get_style)


class Header(object):
    """
    A header row on a column.
    """
    def __init__(self, name, col_num, table):
        self.name = name
        self.col_num = col_num
        self.table = table
    
    def __repr__(self):
        return u"<Header: {0}>".format(self.name)
        
    def __unicode__(self):
        return self.name
    
    def __eq__(self, other):
        if type(other) == type(self):
            return self.name == other.name
        else:
            return self.name == other
    
    def as_th(self):
        return u'<th style="{style}" class="header">{value}</th>'.format(style=self.style or '', value=self)
    
    def _get_style(self):
        try:
            return self.table.style[self.name]
        except KeyError:
            return None
    style = property(_get_style)


def odd_even(num):
    if num % 2 == 0:
        return u"even"
    else:
        return u"odd"
