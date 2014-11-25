import tables as tb
import numpy as np
from scipy.sparse import csr_matrix, coo_matrix, dok_matrix



def createadjacency(images, graph, outputfile):
    """
    Creates an image to image, sparse adjacency matrix and stores it
    in the hdf5

    Parameters
    ----------
    images      list    A list of images to be used in creating the matrix
    graph       dict    The graph representation of the adjacency structure
    outputfile  str     The name/path of the hdf5 file

    Returns
    --------
    """

    image_lookup = {}
    for i, img in enumerate(images):
        image_lookup[img] = i

    idxgraph = {}
    for k, v in graph.iteritems():
        idxgraph[image_lookup[k[0]], image_lookup[k[1]]] = v

    csr_adjacencymatrix = convert(idxgraph)

    #Store the sparse adjacency matrix in hdf5 as CArrays
    store_sparse_mat(csr_adjacencymatrix, 'image_adjacency', store=outputfile)

def convert(term_dict):
    """
    Convert a dictionary with elements of form ('d1', 't1'): 12 to a CSR type matrix.
    The element ('d1', 't1'): 12 becomes entry (0, 0) = 12.
    * Conversion from 1-indexed to 0-indexed.
    * d is row
    * t is column.
    """
    # Create the appropriate format for the COO format.
    data = []
    row = []
    col = []
    for k, v in term_dict.items():
        r = int(k[0])
        c = int(k[1])
        data.append(v)
        row.append(r)
        col.append(c)
    # Create the COO-matrix
    coo = coo_matrix((data,(row,col)))
    # Let Scipy convert COO to CSR format and return
    return csr_matrix(coo)

def store_sparse_mat(m, name, store='store.h5'):
    """
    Stores a sparse, csr matrix into an hdf5 data store.

    Parameters
    -----------
    m       ndarray numpy CSR sparse matrix
    name    str     name of the sparse matrix in the hdf5 file
    store   str     name of the store into which the CArray is packed


    Returns
    --------
    None

    """
    msg = "This code only works for csr matrices"
    assert(m.__class__ == csr_matrix), msg
    with tb.openFile(store,'a') as f:
        for par in ('data', 'indices', 'indptr', 'shape'):
            full_name = '%s_%s' % (name, par)
            try:
                n = getattr(f.root, full_name)
                n._f_remove()
            except AttributeError:
                print "ERROR"

            arr = np.array(getattr(m, par))
            atom = tb.Atom.from_dtype(arr.dtype)
            ds = f.createCArray(f.root, full_name, atom, arr.shape)
            ds[:] = arr
            ds.flush()

def load_sparse_mat(name, store='store.h5'):
    """
    Given a CArray representing a sparse matrix and a hdf5 data store,
    load the sparse matrix into memory

    Parameters
    ----------
    name    str     name of the CArray in the data store
    store   str     name of the hdf5 file

    Returns
    --------
    mi      ndarray CSR sparse matrix
    """

    with tb.openFile(store, 'a') as f:
        pars = []
        for par in ('data', 'indices', 'indptr', 'shape'):
            pars.append(getattr(f.root, '%s_%s' % (name, par)).read())
    m = csr_matrix(tuple(pars[:3]), shape=pars[3])
    return m
