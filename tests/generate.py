from sklearn.datasets import make_checkerboard, make_blobs
from sklearn.utils import check_random_state
from sklearn.metrics import consensus_score
from scipy.stats import multivariate_normal
import numpy as np, numpy.random
import sys
def make_checkerboard(
    shape,
    n_clusters,
    *,
    noise=0.0,
    minval=10,
    maxval=100,
    shuffle=True,
    random_state=None,
):
    """Generate an array with block checkerboard structure for biclustering.

    Read more in the :ref:`User Guide <sample_generators>`.

    Parameters
    ----------
    shape : tuple of shape (n_rows, n_cols)
        The shape of the result.

    n_clusters : int or array-like or shape (n_row_clusters, n_column_clusters)
        The number of row and column clusters.

    noise : float, default=0.0
        The standard deviation of the gaussian noise.

    minval : float, default=10
        Minimum value of a bicluster.

    maxval : float, default=100
        Maximum value of a bicluster.

    shuffle : bool, default=True
        Shuffle the samples.

    random_state : int, RandomState instance or None, default=None
        Determines random number generation for dataset creation. Pass an int
        for reproducible output across multiple function calls.
        See :term:`Glossary <random_state>`.

    Returns
    -------
    X : ndarray of shape `shape`
        The generated array.

    rows : ndarray of shape (n_clusters, X.shape[0])
        The indicators for cluster membership of each row.

    cols : ndarray of shape (n_clusters, X.shape[1])
        The indicators for cluster membership of each column.

    See Also
    --------
    make_biclusters : Generate an array with constant block diagonal structure
        for biclustering.

    References
    ----------
    .. [1] Kluger, Y., Basri, R., Chang, J. T., & Gerstein, M. (2003).
        Spectral biclustering of microarray data: coclustering genes
        and conditions. Genome research, 13(4), 703-716.
    """
    generator = check_random_state(random_state)

    if hasattr(n_clusters, "__len__"):
        n_row_clusters, n_col_clusters = n_clusters
    else:
        n_row_clusters = n_col_clusters = n_clusters

    # row and column clusters of approximately equal sizes
    n_rows, n_cols = shape
    row_sizes = generator.multinomial(
        n_rows, np.repeat(1.0 / n_row_clusters, n_row_clusters)
    )
    col_sizes = generator.multinomial(
        n_cols, np.repeat(1.0 / n_col_clusters, n_col_clusters)
    )

    row_labels = np.hstack(
        [np.repeat(val, rep) for val, rep in zip(range(n_row_clusters), row_sizes)]
    )
    col_labels = np.hstack(
        [np.repeat(val, rep) for val, rep in zip(range(n_col_clusters), col_sizes)]
    )

    result = np.zeros(shape, dtype=np.float64)
    for i in range(n_row_clusters):
        for j in range(n_col_clusters):
            selector = np.outer(row_labels == i, col_labels == j)
            result[selector] += generator.uniform(minval, maxval)

    if noise > 0:
        result += generator.normal(scale=noise, size=result.shape)

    if shuffle:
        result, row_idx, col_idx = _shuffle(result, random_state)
        row_labels = row_labels[row_idx]
        col_labels = col_labels[col_idx]
    print(len(row_labels),len(col_labels))
    # rows = np.vstack(
    #     [
    #         row_labels == label
    #         for label in range(n_row_clusters)
    #         for _ in range(n_col_clusters)
    #     ]
    # )
    # cols = np.vstack(
    #     [
    #         col_labels == label
    #         for _ in range(n_row_clusters)
    #         for label in range(n_col_clusters)
    #     ]
    # )

    return result, row_labels, col_labels
def _shuffle(data, random_state=None):
    generator = check_random_state(random_state)
    n_rows, n_cols = data.shape
    row_idx = generator.permutation(n_rows)
    col_idx = generator.permutation(n_cols)
    result = data[row_idx][:, col_idx]
    return result, row_idx, col_idx

observations=int(sys.argv[1])
features=int(sys.argv[2])
n_clusters=(int(sys.argv[3]),int(sys.argv[4]))
data_shape=(observations,features)
data, rowPartions,columnsPartions = make_checkerboard(
    shape=(observations, features), n_clusters=n_clusters, noise=10, shuffle=False, random_state=42
)

with open("data/synthetic_"+str(data.shape[0])+"_"+str(data.shape[1])+"_"+str(n_clusters[0]*n_clusters[1])+".csv", 'w') as f:
    f.write(','.join([str(x) for x in range(data.shape[1])]))
    f.write("\n")
    for i in range(data.shape[0]):
        line=(','.join([str(y) for y in data[i][:]]))
        f.write(line)
        f.write("\n")

datasets=""
col_size_str='/'.join([str(y) for y in columnsPartions])
row_size_str='/'.join([str(y) for y in rowPartions])
datasets+="synthetic_"+str(data_shape[0])+"_"+str(data_shape[1])+"_"+str(n_clusters[0]*n_clusters[1])+".csv"+","+row_size_str+","+col_size_str+"\n"
file=open("dataset_glob.csv","a")
file.write(datasets)
file.close()
