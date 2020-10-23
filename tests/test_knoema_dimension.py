import pytest
import knoema
import pandas


@pytest.mark.parametrize('dataset_id,dimension_id', [
    ('IMFWEO2020Apr', 'country'),
    ('INTLRRC2017', 'country'),
    ('bjxchy', 'loan-type')])
def test_dimension_metadata(get_client, dataset_id, dimension_id):
    res = knoema.dimension(dataset_id, dimension_id)

    assert res.datasetId == dataset_id
    assert res.id == dimension_id
    assert type(res.members) == pandas.core.frame.DataFrame