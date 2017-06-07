"""This is test module for knoema client"""

import unittest
import datetime
import knoema

class TestKnoemaClient(unittest.TestCase):
    """This is class with knoema client unit tests"""

    def test_getdata_singleseries_bydimid(self):
        """The method is testing getting single series by dimensions ids"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914', subject='ngdp')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 1)

        indx = data_frame.first_valid_index()
        sname = 'Albania - Gross domestic product, current prices (National currency) - A'
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 18.489)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2292.486)

    def test_getdata_multiseries_bydimid(self):
        """The method is testing getting multiple series by dimensions ids"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = 'United States - Gross domestic product, current prices (National currency) - A'
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2862.475)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 23760.331)

    def test_getdata_multiseries_bydimname(self):
        """The method is testing getting data by dimensions names"""

        subj_names = 'Gross domestic product, current prices (National currency);population (persons)'
        data_frame = knoema.get('IMFWEO2017Apr', country='albania;afghanistan;united states', subject=subj_names)
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = 'United States - Gross domestic product, current prices (National currency) - A'
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2862.475)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 23760.331)

    def test_getdata_multiseries_bydimid_range(self):
        """The method is testing getting multiple series by dimensions ids and time range"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp', timerange='2015-2020')
        self.assertEqual(data_frame.shape[0], 6)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = 'United States - Gross domestic product, current prices (National currency) - A'
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 18036.650)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 22063.044)

    def test_getdata_singleseries_difffrequencies_bydimid(self):
        """The method is testing getting single series on different frequencies by dimensions ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT', subject='BSCI', measure='blsa')
        self.assertEqual(data_frame.shape[0], 388)
        self.assertEqual(data_frame.shape[1], 2)

        indx = data_frame.first_valid_index()
        sname = 'Austria - Confidence indicators - Balance, s.a. - M'
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, -5.0)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 1.9)

        indx = data_frame.first_valid_index()
        sname = 'Austria - Confidence indicators - Balance, s.a. - Q'
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, -5.233333)

        value = data_frame.get_value(datetime.datetime(2017, 1, 1), sname)
        self.assertEqual(value, 1.566667)

    def test_getdata_multiseries_singlefrequency_bydimid(self):
        """The method is testing getting mulitple series with one frequency by dimensions ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;AU', subject='BSCI', measure='blsa', frequency='Q')
        self.assertEqual(data_frame.shape[0], 204)
        self.assertEqual(data_frame.shape[1], 2)

        sname = 'Austria - Confidence indicators - Balance, s.a. - Q'
        value = data_frame.get_value(datetime.datetime(2017, 1, 1), sname)
        self.assertEqual(value, 1.566667)

    def test_getdata_multiseries_multifrequency_bydimid(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimensions ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;AU', subject='BSCI', measure='blsa', frequency='Q;M')
        self.assertEqual(data_frame.shape[0], 463)
        self.assertEqual(data_frame.shape[1], 3)

        sname = 'Austria - Confidence indicators - Balance, s.a. - M'
        value = data_frame.get_value(datetime.datetime(2017, 3, 1), sname)
        self.assertEqual(value, 2.4)

    def test_getdata_multiseries_multifrequency_bydimid_range(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimensions ids with time range"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;BE', subject='BSCI', measure='blsa', frequency='Q;M', timerange='2010M1-2015M12')
        self.assertEqual(data_frame.shape[0], 72)
        self.assertEqual(data_frame.shape[1], 3)

        sname = 'Austria - Confidence indicators - Balance, s.a. - M'
        value = data_frame.get_value(datetime.datetime(2012, 12, 1), sname)
        self.assertEqual(value, -12.4)

    def test_none_dataset(self):
        """The method is testing if dataset set up as None"""

        with self.assertRaises(ValueError) as context:
            knoema.get(None)

        self.assertTrue('Dataset id is not specified' in str(context.exception))

    def test_no_selection(self):
        """The method is testing if there is no dimensions specified"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr')

        self.assertTrue('Dimensions members are not specified' in str(context.exception))

    def test_wrong_dimension(self):
        """The method is testing if there is wrong dimension name is specified"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', indicator='LP;NGDP')

        self.assertTrue('Dimension with name indicator is not found' in str(context.exception))

    def test_empty_dimension_selection(self):
        """The method is testing if there are no elements in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', country='', subject='LP;NGDP')

        self.assertTrue('Selection for dimension Country is empty' in str(context.exception))

    def test_wrong_dimension_selection(self):
        """The method is testing if there are incorrect in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', country='914;512;111', subject='L1P;N1GDP')

        self.assertTrue('Selection for dimension Subject is empty' in str(context.exception))

    def test_get_data_from_flat_dataset(self):
        """The method is testing load data from flat dataset"""

        data_frame = knoema.get('cblymmf', Country='Albania;Australia', Keyword='FGP;TWP;TRP')
        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 4)

        sname = 'Albania - FGP - D'
        value = data_frame.get_value('All time', sname)
        self.assertEqual(value, 8.0)
