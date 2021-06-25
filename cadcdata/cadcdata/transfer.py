# -*- coding: utf-8 -*-
# ************************************************************************
# *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
# *
# *  (c) 2015.                            (c) 2015.
# *  Government of Canada                 Gouvernement du Canada
# *  National Research Council            Conseil national de recherches
# *  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
# *  All rights reserved                  Tous droits réservés
# *
# *  NRC disclaims any warranties,        Le CNRC dénie toute garantie
# *  expressed, implied, or               énoncée, implicite ou légale,
# *  statutory, of any kind with          de quelque nature que ce
# *  respect to the software,             soit, concernant le logiciel,
# *  including without limitation         y compris sans restriction
# *  any warranty of merchantability      toute garantie de valeur
# *  or fitness for a particular          marchande ou de pertinence
# *  purpose. NRC shall not be            pour un usage particulier.
# *  liable in any event for any          Le CNRC ne pourra en aucun cas
# *  damages, whether direct or           être tenu responsable de tout
# *  indirect, special or general,        dommage, direct ou indirect,
# *  consequential or incidental,         particulier ou général,
# *  arising from the use of the          accessoire ou fortuit, résultant
# *  software.  Neither the name          de l'utilisation du logiciel. Ni
# *  of the National Research             le nom du Conseil National de
# *  Council of Canada nor the            Recherches du Canada ni les noms
# *  names of its contributors may        de ses  participants ne peuvent
# *  be used to endorse or promote        être utilisés pour approuver ou
# *  products derived from this           promouvoir les produits dérivés
# *  software without specific prior      de ce logiciel sans autorisation
# *  written permission.                  préalable et particulière
# *                                       par écrit.
# *
# *  This file is part of the             Ce fichier fait partie du projet
# *  OpenCADC project.                    OpenCADC.
# *
# *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
# *  you can redistribute it and/or       vous pouvez le redistribuer ou le
# *  modify it under the terms of         modifier suivant les termes de
# *  the GNU Affero General Public        la “GNU Affero General Public
# *  License as published by the          License” telle que publiée
# *  Free Software Foundation,            par la Free Software Foundation
# *  either version 3 of the              : soit la version 3 de cette
# *  License, or (at your option)         licence, soit (à votre gré)
# *  any later version.                   toute version ultérieure.
# *
# *  OpenCADC is distributed in the       OpenCADC est distribué
# *  hope that it will be useful,         dans l’espoir qu’il vous
# *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
# *  without even the implied             GARANTIE : sans même la garantie
# *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
# *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
# *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
# *  General Public License for           Générale Publique GNU Affero
# *  more details.                        pour plus de détails.
# *
# *  You should have received             Vous devriez avoir reçu une
# *  a copy of the GNU Affero             copie de la Licence Générale
# *  General Public License along         Publique GNU Affero avec
# *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
# *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
# *                                       <http://www.gnu.org/licenses/>
# *
# ************************************************************************

from six.moves.urllib.parse import urlparse
from six import string_types, iteritems
from lxml import etree
import os

# VOSpace versions and schema (loaded as needed)
VOSPACE_20 = 20
VOSPACE_21 = 21
VOSPACE_SCHEMA = {VOSPACE_20: None,
                  VOSPACE_21: None}

# Other constants from the VOSpace standard
PROTOCOL_HTTPS_GET = 'ivo://ivoa.net/vospace/core#httpsget'
PROTOCOL_HTTPS_PUT = 'ivo://ivoa.net/vospace/core#httpsput'
DIRECTION_PROTOCOL_MAP = {'pushToVoSpace': [PROTOCOL_HTTPS_PUT],
                          'pullFromVoSpace': [PROTOCOL_HTTPS_GET]}

# supported transfer schemes
TRANSFER_SCHEMES = ['vos', 'ad', 'mast']

# The list of NODE_PROPERTIES is extensive. Any properties listed here are
# simply special ones that we plan to handle (e.g., length can only be set in
# > VOSPACE_21)
# Perhaps a new thing to add: md5? (to verify things without separate HEAD)
NODE_PROPERTIES = {
    'LENGTH': ('uri', 'ivo://ivoa.net/vospace/core#length', VOSPACE_21)
}

# Lookup NODE_PROPERTIES given the property value (e.g., URI)
NODE_PROPERTIES_LOOKUP = {NODE_PROPERTIES[x][1]: x for x in NODE_PROPERTIES}

# XML-related constants
VOSPACE_NS = {VOSPACE_20: 'http://www.ivoa.net/xml/VOSpace/v2.0',
              VOSPACE_21: 'http://www.ivoa.net/xml/VOSpace/v2.1'}

VOSPACE_SCHEMA_RESOURCE = {VOSPACE_20: 'VOSpace-2.0.xsd',
                           VOSPACE_21: 'VOSpace-2.1.xsd'}

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
DATA_PKG = 'data'


class TransferError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Protocol(object):
    """ Container for data transfer URIs (put/get) and endpoints (get) """

    def __init__(self, uri, endpoint=None):
        self.uri = uri
        if endpoint:
            self.endpoint = endpoint
        else:
            self.endpoint = None


class Transfer(object):
    """ VOSpace transfer job description """

    def __init__(self, target, direction, version=None, properties=None,
                 protocols=None):
        """ Initialize a Transfer description

        target    -- URI of remote file
        direction -- pushToVoSpace or pullFromVoSpace
        """
        self.target = None
        self.direction = None
        self.properties = dict()
        self.protocols = []

        if version is None:
            self.version = VOSPACE_20
        else:
            self.set_version(version)

        self.set_target(target)
        self.set_direction(direction)

        # Optional properties from dictionary
        if properties:
            for prop in properties:
                self.set_property(prop, properties[prop])

        # Optionally set protocols
        if protocols:
            for protocol in protocols:
                self.add_protocol(protocol)
        elif self.direction == 'pushToVoSpace':
            # If we're doing a put and no protocol specified, set default
            for p in DIRECTION_PROTOCOL_MAP['pushToVoSpace']:
                self.add_protocol(Protocol(p))
        elif self.direction == 'pullFromVoSpace':
            # If we're doing a pull and no protocol specified, set default
            for p in DIRECTION_PROTOCOL_MAP['pullFromVoSpace']:
                self.add_protocol(Protocol(p))

    def set_version(self, version_in):
        """ Set a valid VOSpace version with validation. """

        if version_in in VOSPACE_SCHEMA:
            self.version = version_in
        else:
            raise TransferError("Invalid VOSpace version %i specified.")

    def set_target(self, target_in):
        """ Set target with basic validation """

        scheme = urlparse(target_in).scheme.lower()
        if scheme not in TRANSFER_SCHEMES:
            raise TransferError(
                'Target scheme {} not in the supported group {}'.
                format(scheme, TRANSFER_SCHEMES))
        self.target = target_in

    def set_direction(self, direction_in):
        """ Set direction

        direction_in -- pushToVoSpace or pullFromVoSpace
        """

        if direction_in not in DIRECTION_PROTOCOL_MAP:
            raise TransferError("Direction %s must be one of: %s" %
                                (direction_in, ', '.join(
                                    [k for k in DIRECTION_PROTOCOL_MAP])))

        self.direction = direction_in

    def get_endpoints(self):
        """ Return ordered list of endpoints """
        return [prot.endpoint for prot in self.protocols]

    def add_protocol(self, protocol):
        """ Add to ordered list of protocols """

        assert isinstance(protocol, Protocol)

        if protocol.uri and \
                (protocol.uri not in DIRECTION_PROTOCOL_MAP[self.direction]):
            raise TransferError(
                "Protocol URI, %s, incompatible with transfer direction, %s."
                % (protocol.uri, self.direction))

        self.protocols.append(protocol)

    def get_property(self, prop):
        """ Return a property """

        return self.properties[prop]

    def set_property(self, prop, value):
        """ Set a property. If a handled property, perform version check """

        if prop in NODE_PROPERTIES:
            (k, v, vs) = NODE_PROPERTIES[prop]
            if self.version < vs:
                raise TransferError(
                    "%s may only be set in VOSpace documents version >= %i"
                    % (prop, vs))

        assert isinstance(value, string_types)

        self.properties[prop] = value


class TransferReaderError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class TransferReader(object):
    """ Construct a Transfer object from XML source """

    def __init__(self, validate=False):
        self.validate = validate

    def read(self, xml_string):
        """ Read XML document string and return a Transfer object """

        xml = etree.fromstring(xml_string)

        # Get the VOSpace version by performing a reverse name lookup
        # on the namespace string
        try:
            ns = xml.nsmap['vos']
            version = dict((v, k) for k, v in iteritems(VOSPACE_NS))[ns]
        except Exception:
            raise TransferReaderError(
                'Unable to establish the VOSpace version of transfer document')

        vos = '{%s}' % ns  # VOS namespace string

        # Schema validation now that we know the version
        if self.validate:
            if VOSPACE_SCHEMA[version] is None:
                # .xsd hasn't been loaded in yet
                filepath = os.path.join(os.path.join(THIS_DIR, DATA_PKG),
                                        VOSPACE_SCHEMA_RESOURCE[version])

                try:
                    with open(filepath) as f:
                        schema_xml = etree.parse(f)
                        VOSPACE_SCHEMA[version] = etree.XMLSchema(schema_xml)
                except Exception as e:
                    raise TransferReaderError(
                        'Unable to load schema %s: %s' %
                        (filepath, str(e)))
            VOSPACE_SCHEMA[version].assertValid(xml)

        # Continue with required nodes
        try:
            target = xml.find(vos + 'target').text
        except Exception:
            raise TransferReaderError(
                'Unable to find a target in the transfer document')

        try:
            direction = xml.find(vos + 'direction').text
        except Exception:
            raise TransferReaderError(
                'Unable to find direction in the transfer document')

        # Protocols
        protocols = []
        for p in xml.findall(vos + 'protocol'):
            uri = p.attrib['uri']
            e = p.find(vos + 'endpoint')
            if e is not None:
                endpoint = e.text
            else:
                endpoint = None
            protocols.append(Protocol(uri, endpoint=endpoint))

        # Properties
        properties = dict()
        for p in xml.findall(vos + 'param'):
            # We only expect one key per parameter
            key = p.attrib.keys()[0]
            value = p.attrib[key]

            try:
                # Try a reverse lookup to find handled NODE_PROPERTY
                prop = NODE_PROPERTIES_LOOKUP[value]
                properties[prop] = p.text
            except Exception:
                properties['%s=%s' % (key, value)] = p.text

        # Create the transfer object
        return Transfer(target, direction, version=version,
                        properties=properties, protocols=protocols)


class TransferWriterError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class TransferWriter(object):
    """ Render a Transfer object as an XML string """

    def write(self, transfer):
        """ Generate an XML string from a Transfer object """

        assert isinstance(transfer, Transfer)

        # Create the root node
        try:
            ns = VOSPACE_NS[transfer.version]  # namespace URI
            nsmap = {'vos': ns}  # map for document
            vos = '{%s}' % ns  # VOS namespace string

        except Exception:
            raise TransferWriterError(
                'Unexpected transfer version %i encountered'
                % transfer.version)

        xml = etree.Element(vos + 'transfer', nsmap=nsmap)

        # Other required nodes
        target = etree.SubElement(xml, vos + 'target', nsmap=nsmap)
        target.text = transfer.target

        direction = etree.SubElement(xml, vos + 'direction', nsmap=nsmap)
        direction.text = transfer.direction

        # Protocols
        for pt in transfer.protocols:
            attrib = {'uri': pt.uri}

            protocol = etree.SubElement(xml, vos + 'protocol', attrib=attrib,
                                        nsmap=nsmap)
            if pt.endpoint:
                endpoint = etree.SubElement(protocol, vos + 'endpoint',
                                            nsmap=nsmap)
                endpoint.text = pt.endpoint

        # Properties
        for prop in transfer.properties:
            try:
                (key, val, ver) = NODE_PROPERTIES[prop]
            except Exception:
                # An unhandled property. We need to split the string
                # into key/val
                (key, val) = prop.split('=')

            attrib = {key: val}
            param = etree.SubElement(xml, vos + 'param', attrib=attrib,
                                     nsmap=nsmap)
            param.text = transfer.properties[prop]

        return etree.tostring(xml, encoding='UTF-8', pretty_print=True)
