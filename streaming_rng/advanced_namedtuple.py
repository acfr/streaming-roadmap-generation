# Py 2/3 compat imports
from __future__ import print_function, division, absolute_import
from future.builtins import dict, list, object, range, str, bytes, filter, map, zip, ascii, chr, hex, input, next, oct, open, pow, round, super
from future.standard_library import install_aliases; install_aliases()
# This file is compatible with Python 2 and Python 3, please keep it so

__author__ = "Konstantin Seiler, The University of Sydney"
__copyright__ = "Copyright Technological Resources Pty Ltd (a Rio Tinto Group company), 2019"

import re as _re
import tokenize as _tokenize
import sys as _sys
from keyword import iskeyword as _iskeyword
from six import string_types
import inspect


if _sys.version_info.major < 3:
    from numpy import isfinite as _isfinite
else:
    from math import isfinite as _isfinite


def _isidentifier(some_string):
    if _sys.version_info.major < 3:
        return (_re.match(_tokenize.Name + '$', some_string) and not _iskeyword(some_string))
    else:
        return some_string.isidentifier()


class _NoValueGiven(object):
    pass


def _named_tuple_unpickle_helper(named_tuple_class, keyword_arguments):
    """A helper that can be used by __reduce__ of a namedtuple to reconstruct
    the object using a key-value dictionary. This allows for some backwards
    compatibility when fields (with default values) are added to the tuple
    or fields are rearranged. Then, old pickles can still be loaded."""

    return named_tuple_class(_run_consistency_checks=False, **keyword_arguments)


_class_template = """\
from __future__ import print_function, absolute_import, division
from future import standard_library
standard_library.install_aliases()
from builtins import super

from builtins import property as _property, tuple as _tuple
from operator import itemgetter as _itemgetter
from collections import OrderedDict
from {this_module_name} import _NoValueGiven

class {typename}({baseclass_name}):
    {doc_string!r}

    __slots__ = ()

    _fields = {field_names!r}

    _consistency_check_functions = []

    def __new__(_cls, {arg_list_with_defaults}{comma_if_arg_list_with_defaults_not_empty} _run_consistency_checks={run_consistency_checks_default}):
        'Create new instance of {typename}({arg_list})'

        self = _tuple.__new__(_cls, ({arg_list}))

{mandatory_args_check}

        if _run_consistency_checks:
            self._run_consistency_checks()

        return self


    @classmethod
    def _make(cls, iterable, new=tuple.__new__, len=len, _run_consistency_checks={run_consistency_checks_default}):
        'Make a new {typename} object from a sequence or iterable'
        result = new(cls, iterable)
        if len(result) != {num_fields:d}:
            raise TypeError('Expected {num_fields:d} arguments, got %d' % len(result))
        if _run_consistency_checks:
            result._run_consistency_checks()
        return result

    def _replace(_self, **kwds):
        'Return a new {typename} object replacing specified fields with new values'

        if '_run_consistency_checks' in kwds:
            _run_consistency_checks = kwds.pop('_run_consistency_checks')
        else:
            _run_consistency_checks = {run_consistency_checks_default}

        result = _self._make(map(kwds.pop, _self._fields, _self), _run_consistency_checks=False)

        if kwds:
            raise ValueError('Got unexpected field names: %r' % list(kwds))

        if _run_consistency_checks:
            result._run_consistency_checks()

        return result

    def __repr__(self):
        'Return a nicely formatted representation string'
        result = self.__class__.__name__ + '('
        for index, name in enumerate(self._fields):
            if index > 0:
                result += ', '
            result += name + '=' + repr(self[index])
        result += ')'
        return result

    def __str__(self):
        'Return a nicely formatted representation string'
        result = '{typename}' + '('
        for index, name in enumerate(self._fields):
            if index > 0:
                result += ', '
            result += name + '=' + str(self[index])
        result += ')'
        return result

    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values.'
        return OrderedDict(zip(self._fields, self))

    def __reduce__(self):
        return _named_tuple_unpickle_helper, (type(self), dict(zip(self._fields, self)))

    def _run_consistency_checks(self):
        if hasattr(super({typename}, self), '_run_consistency_checks'):
            super({typename}, self)._run_consistency_checks()
        for check_function in {typename}._consistency_check_functions:
            check_function(self)

{field_defs}

    @classmethod
    def _add_consistency_check_function(cls, check_function):
        cls._consistency_check_functions.append(check_function)
        return check_function

"""

_docstring_template = '{typename}({arg_list_with_defaults})'

_field_template = '''\
    {name} = _property(_itemgetter({index:d}), doc='Alias for field number {index:d}')

'''

_mandatory_args_check_template = '''\
        if self.{name} is _NoValueGiven:
            raise TypeError("{typename}() missing 1 required argument: '{name}'")

'''


def namedtuple(typename, field_names, verbose=False, rename=False, module=None, doc=None, baseclass=tuple, run_consistency_checks_by_default=False):
    """Returns a new subclass of tuple with named fields.

    It is a drop-in replacement for the namedtuple of Python's collections module.
    However, it does have some additional features:
        * It allows for inheriting from another namedtuple (even ones created by the collections module)
        * It allows to provide a doc string which is incorporated into the class
        * It allows to specify default values for the constructor
        * It allows to specify a function that is executed after creation (this can be used to error-check the provided values)

    :arg string typename: the name of the class that is being created.
    :arg field_names: the names of the field. This can be specified as either:
        1) a string of comma-separated field names
        2) an iterable of strings
        3) an iterable of tuples. Each tuple has to be a pair whith the first entry
            being a string specifying the field name and the sedonf entry being
            a default value to be incorporated into the constructor.
        Note: options 2) and 3) may be combined (i.e. some fields are specified as strings
        while others are tuples).
    :arg bool rename: rename invalid field names.
    :arg module: the module the class should be injected into. Defaults to the caller's namespace.
    :arg doc: a string that is used as doc-string for the class
    :arg class baseclass: a class which is used as baseclass. Must either be tuple or a class
        which is a namedtuple, either created by collections or this function.
    :arg function after_creation_hook: a function which is called after a class instance has been created.
        The class instance is passed as first argument, so the semantics are the same as with a class method.

    Note: the after_creation_hook can also be added after class creation using the _set_after_creation_hook
        decorator which is defined at class level. This results in more readable source code.

    Note: the after_creation_hook is kept separate for each sub-class. Thus, multiple hooks can be
        in effect within a class hierarchy.


    >>> Point = namedtuple('Point', ['x', 'y'])
    >>> Point.__doc__                   # docstring for the new class
    'Point(x, y)'
    >>> p = Point(11, y=22)             # instantiate with positional args or keywords
    >>> p[0] + p[1]                     # indexable like a plain tuple
    33
    >>> x, y = p                        # unpack like a regular tuple
    >>> x, y
    (11, 22)
    >>> p.x + p.y                       # fields also accessible by name
    33
    >>> d = p._asdict()                 # convert to a dictionary
    >>> d['x']
    11
    >>> Point(**d)                      # convert from a dictionary
    Point(x=11, y=22)
    >>> p._replace(x=100)               # _replace() is like str.replace() but targets named fields
    Point(x=100, y=22)

    """

    # Validate the field names.  At the user's option, either generate an error
    # message or automatically replace the field name with a valid name.
    if isinstance(field_names, string_types):
        field_names = field_names.replace(',', ' ').split()
    else:
        field_names = list(field_names)

    all_field_names = []
    additional_field_names = []     # field names that are new in this class (i.e. they are not in the base class)
    baseclass_field_count = 0
    default_values = dict()
    default_value_was_given = False
    field_index = 0

    mandatory_args_check = ''

    namespace = dict()  # the namespace we pass to exec when we create the new class.

    seen = set()

    # get fields from the base class
    if hasattr(baseclass, '_fields'):
        # the baseclass seems to be a namedtuple.
        # we extract the fields it already has as well
        # as the default values attached to the constructor.

        baseclass_field_count = len(baseclass._fields)

        for index, name in enumerate(baseclass._fields):

            all_field_names.append(name)
            seen.add(name)

            field_index += 1

        if baseclass.__new__.__defaults__:

            if _sys.version_info.major < 3:
                new_argument_names = inspect.getargspec(baseclass.__new__).args
            else:
                new_argument_names = tuple(inspect.signature(baseclass.__new__).parameters)

            default_offset = len(new_argument_names) - len(baseclass.__new__.__defaults__)

            for argument_index, argument_name in enumerate(new_argument_names):
                if argument_index == 0:
                    # this is the cls argument
                    continue

                if argument_index - default_offset >= 0 and argument_name in baseclass._fields:
                    default_values[argument_name] = baseclass.__new__.__defaults__[argument_index-default_offset]
                    default_value_was_given = True

    # process the field names and default values
    for item in field_names:

        if isinstance(item, tuple):
            if not len(item) == 2:
                raise ValueError("Default arguments must be given as name-value pair. Tuple of length %i found." % (len(item),))
            name, default_value = item
            default_value_was_given = True

        else:
            name = item

            if default_value_was_given:
                mandatory_args_check += _mandatory_args_check_template.format(name=name, typename=typename)
                default_value = _NoValueGiven
            else:
                default_value = None

        name = str(name)

        if rename:
            if (not _isidentifier(name)
                or _iskeyword(name)
                or name.startswith('_')
                or name in seen):
                name = '_%d' % index

        if name.startswith('_') and not rename:
            raise ValueError('Field names cannot start with an underscore: '
                             '%r' % name)
        if name in seen:
            raise ValueError('Encountered duplicate field name: %r' % name)

        all_field_names.append(name)
        additional_field_names.append(name)
        if default_value_was_given:
            default_values[name] = default_value
        seen.add(name)

        field_index += 1

    # ensure the typename is a string.
    typename = str(typename)

    # sanity check
    for name in [typename] + all_field_names:
        if type(name) is not str:
            raise TypeError('Type names and field names must be strings')
        if not _isidentifier(name):
            raise ValueError('Type names and field names must be valid '
                             'identifiers: %r' % name)
        if _iskeyword(name):
            raise ValueError('Type names and field names cannot be a '
                             'keyword: %r' % name)

    # create a string (source code) representation of the argument list of the constructor
    arg_list_with_defaults = ""

    for index, name in enumerate(all_field_names):
        if index > 0:
            arg_list_with_defaults += ', '
        arg_list_with_defaults += name
        if name in default_values:
            default_value = default_values[name]

            if default_value is _NoValueGiven:
                default_value_string = '_NoValueGiven'

            elif type(default_value) is float:
                if _isfinite(default_value):
                    default_value_string = repr(default_value)
                else:
                    default_value_string = "float('{!r}')".format(default_value)
            elif (default_value is None
                  or type(default_value) in (int, bool)
                  or (_sys.version_info.major >= 3 and type(default_value) is str)
                  or (_sys.version_info.major < 3 and type(default_value) is long)
                  or (type(default_value) is (tuple, frozenset) and not default_value)
            ):
                default_value_string = repr(default_value)

            else:
                # the default value is some other (potentially mutable) object. Thus,
                # we inject the object into the namespace when we create the class
                # and refer to it by name.
                default_arg_name = '_default_argument_%i' % index
                namespace[default_arg_name] = default_value
                default_value_string = default_arg_name

            arg_list_with_defaults += '=' + default_value_string

    # create the defauld doc string if none was given.
    if doc is None:
        doc = _docstring_template.format(typename=typename, arg_list_with_defaults=arg_list_with_defaults)

    # get the name of the base class
    baseclass_name = baseclass.__name__

    # Fill-in the class template
    class_definition = _class_template.format(
        this_module_name=__name__,
        typename=typename,
        baseclass_name=baseclass_name,
        doc_string=doc,
        field_names=tuple(all_field_names),
        num_fields=len(all_field_names),
        baseclass_field_count=baseclass_field_count,
        arg_list=repr(tuple(all_field_names)).replace("'", "")[1:-1],
        arg_list_with_defaults=arg_list_with_defaults,
        comma_if_arg_list_with_defaults_not_empty="" if arg_list_with_defaults=="" else ",",
        field_defs='\n'.join([_field_template.format(index=index+baseclass_field_count, name=name)
                             for index, name in enumerate(additional_field_names)]),
        mandatory_args_check=mandatory_args_check,
        run_consistency_checks_default='True' if run_consistency_checks_by_default else 'False',
    )

    # Execute the template string in a temporary namespace and support
    # tracing utilities by setting a value for frame.f_globals['__name__']
    namespace['__name__'] = 'namedtuple_%s' % typename
    namespace[baseclass_name] = baseclass
    namespace['_named_tuple_unpickle_helper'] = _named_tuple_unpickle_helper

    if verbose:
        print(namespace)
        print(class_definition)

    exec(class_definition, namespace)
    result = namespace[typename]
    result._source = class_definition

    # For pickling to work, the __module__ variable needs to be set to the frame
    # where the named tuple is created.  Bypass this step in environments where
    # sys._getframe is not defined (Jython for example) or sys._getframe is not
    # defined for arguments greater than 0 (IronPython), or where the user has
    # specified a particular module.
    if module is None:
        try:
            module = _sys._getframe(1).f_globals.get('__name__', '__main__')
        except (AttributeError, ValueError):
            pass
    if module is not None:
        result.__module__ = module

    return result
