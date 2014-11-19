from collections import defaultdict
from itertools import combinations

import tables as tb
import pandas as pd
import ControlNetFileV0002_pb2 as spec
from adjacencymatrix import createadjacency

class ControlPoint(tb.IsDescription):
    """
    A class that maps the cnet file to a hdf5 file.

    Currently, this class is incomplete, with some data time information not
    being read.  Additionally, this class should be extended to include
    the mapping of some of the numeric values back to textual values.  This can
    be accomplished, I believe, by adding methods to the mapping.
    """
    pointid = tb.StringCol(64)  #Why string?  #Int or float?  In the latter case,

    pointtype = tb.StringCol(12)
    choosername = tb.StringCol(36)
    datetime = tb.StringCol(36)  #Should be datetime
    referenceindex = tb.Int32Col()
    apriorisurface_pointsource = tb.StringCol(32)
    apriorisurface_pointfile = tb.StringCol(128)
    aprioriradius_pointsource = tb.StringCol(32)
    aprioriradius_pointfile = tb.StringCol(128)

    latitudeconstrained = tb.BoolCol()
    longitudeconstrained = tb.BoolCol()
    radiusconstrained = tb.BoolCol()

    #apriorilatitude = tb.FloatCol()
    apriorix = tb.FloatCol()
    #apriorilongitude = tb.FloatCol()
    aprioriy = tb.FloatCol()
    #aprioriradius = tb.FloatCol()
    aprioriz = tb.FloatCol()

    #adjustedlatitude = tb.FloatCol()
    adjustedx = tb.FloatCol()
    #adjustedlongitude = tb.FloatCol()
    adjustedy = tb.FloatCol()
    #adjustedradius = tb.FloatCol()
    adjustedz = tb.FloatCol()

    #aprioricovar = tb.Float32Col(shape=6)
    #adjustedcovar = tb.Float32Col(shape=6)

    #Measure
    serialnumber = tb.StringCol(256)
    measuretype = tb.StringCol(32)
    choosername = tb.StringCol(32)
    datetime = tb.StringCol(64)  #should be datetime
    editlock = tb.BoolCol()
    ignore = tb.BoolCol()
    jigsawrejected = tb.BoolCol()
    diameter = tb.FloatCol()
    sample = tb.FloatCol()
    line = tb.FloatCol()

    samplesigma = tb.FloatCol()
    linesigma = tb.FloatCol()

    apriorisample = tb.FloatCol()
    aprioriline = tb.FloatCol()
    sampleresidual = tb.FloatCol()
    lineresidual = tb.FloatCol()
    goodnessoffit = tb.FloatCol()
    #reference = tb.BoolCol()


def createhdf5(inputcnet, outputfile):
    """
    Creates the hdf5 file containing all of the observations and
    an image adjacency matrix.

    Parameters
    -----------
    inputcnet       str the name/path of the input controlnet in the isis binary format (pseudo-protocol buffers)
    outputfile      str the name/path of the output file

    """
    h5file = tb.open_file(outputfile, mode='w', title='cnet')
    image_group = h5file.create_group('/', 'Images', 'Lookup table of images to image serials')

    #Maybe kick down a level
    free_group = h5file.create_group('/', 'Free', 'Points of type free')
    #fixed_group = h5file.create_group('/', 'Fixed', 'Points of type fixed')
    #constrained_group = h5file.create_group('/', 'Constrained', 'Points of type constrained')

    #Table within groups
    freetable = h5file.create_table(free_group, 'controlmeasure', ControlPoint, 'Free Control Points')
    #constrainedtable = h5file.create_table(constrained_group, 'controlmeasure', ControlPoint, 'Constrainted Control Points')
    #fixedtable = h5file.create_table(fixed_group, 'controlmeasure', ControlPoint, 'Fixed Control Points')

    #For now - pack all points into a single free table.

    #Create a row object in the free table.
    freerow = freetable.row

    graph = defaultdict(int)
    images = set([])
    #Open the input dataset
    with open('Elysium_DayIR_Final.net', 'r') as stream:
        header = spec.ControlNetFileHeaderV0002()
        stream.seek(65536)  #Why are we doing more custom stuff?
        headerbinary = stream.read(513513)
        header.ParseFromString(headerbinary)
        binarysizes = header.pointMessageSizes
        for i, chunk in enumerate(binarysizes):
            #Read the message
            binary = stream.read(chunk)
            cp = spec.ControlPointFileEntryV0002()
            cp.ParseFromString(binary)
            adj = []
            #Loop over the points
            for j, m in enumerate(cp.measures):

                freerow['pointid'] = cp.id
                freerow['pointtype'] = cp.type
                freerow['choosername'] = cp.chooserName
                #datetime = tb.StringCol(36)  #Should be datetime
                freerow['referenceindex'] = cp.referenceIndex
                freerow['apriorisurface_pointsource'] = cp.aprioriSurfPointSource
                freerow['apriorisurface_pointfile'] = cp.aprioriSurfPointSourceFile
                freerow['aprioriradius_pointsource'] = cp.aprioriRadiusSource
                freerow['aprioriradius_pointfile'] = cp.aprioriRadiusSourceFile

                freerow['latitudeconstrained'] = cp.latitudeConstrained
                freerow['longitudeconstrained'] = cp.longitudeConstrained
                freerow['radiusconstrained'] = cp.radiusConstrained

                #apriorilatitude = cp.
                freerow['apriorix'] = cp.aprioriX
                #apriorilongitude = tb.FloatCol()
                freerow['aprioriy'] = cp.aprioriY
                #aprioriradius = tb.FloatCol()
                freerow['aprioriz'] = cp.aprioriZ

                #adjustedlatitude = tb.FloatCol()
                freerow['adjustedx'] = cp.adjustedX
                #adjustedlongitude = tb.FloatCol()
                freerow['adjustedy'] = cp.adjustedY
                #adjustedradius = tb.FloatCol()
                freerow['adjustedz'] = cp.adjustedZ

                '''
                arr = cp.aprioriCovar
                if len(arr) != 6:
                    freerow['aprioricovar'] = np.zeros(6)
                else:
                    freerow['aprioricovar'] = arr
                print freerow['aprioricovar']

                arr = cp.adjustedCovar
                if len(arr) != 6:
                    freerow['adjustedcovar'] = np.zeros(6)
                else:
                    freerow['adjustedcovar'] = arr

                print freerow['adjustedcovar']
                '''

                freerow['serialnumber'] = m.serialnumber
                freerow['measuretype'] = m.type
                freerow['choosername'] = m.choosername
                #datetime = tb.StringCol(64)  #should be datetime
                freerow['editlock'] = m.editLock
                freerow['ignore'] = m.ignore
                freerow['jigsawrejected'] = m.jigsawRejected
                freerow['diameter'] = m.diameter
                freerow['sample'] = m.sample
                freerow['line'] = m.line

                freerow['samplesigma'] = m.samplesigma
                freerow['linesigma'] = m.linesigma

                freerow['apriorisample'] = m.apriorisample
                freerow['aprioriline'] = m.aprioriline
                freerow['sampleresidual'] = m.sampleResidual
                freerow['lineresidual'] = m.lineResidual

                adj.append(m.serialnumber)
                images.add(m.serialnumber)

                #Append the row to the table.  This is a bulk insert style
                freerow.append()


            for i,j in combinations(adj, 2):
                graph[i,j] += 1
                graph[j,i] += 1

            #Flush the table to explicitly write to disk
            freetable.flush()

    h5file.close()
    images = list(images)

    #Create the spare adjacency matrix
    createadjacency(images, graph, outputfile)

def load(inputfile, tablename):
    """
    Loads an hdf5 file into a pandas dataframe

    Parameters
    -----------
    inputfile   str name/path of the input hdf5 file
    tablename   str

    Returns
    -------
    df          obj pandas data frame
    """

    return pd.read_hdf(inputfile, tablename)


