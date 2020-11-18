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

def test_dimension_metadata_not_all_fields_in_items(get_client):
    dataset_id = 'kaziajg'
    dimension_name = 'Location'
    res = knoema.dimension(dataset_id, dimension_name)

    assert res.datasetId == dataset_id
    assert res.name == dimension_name
    assert type(res.members) == pandas.core.frame.DataFrame
    assert len(res.members.axes[1]) == 12
