import knoema

def test_calculations_in_dimention(get_client):
    data = knoema.get('IMFWEO2020Apr', **{
    'Country': 'Armenia',
    'Subject': '@SUM;NGDP_RPCH;PCPIPCH'})

    assert data.columns.values[0] == ('Armenia', 'Sum of 2', 'A')
    assert data.shape == (29, 1)