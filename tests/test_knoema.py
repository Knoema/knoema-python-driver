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

    def test_get_data_from_dataset_with_multiword_dimnames(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names"""

        data_frame = knoema.get('FDI_FLOW_CTRY', **{'Reporting country': 'AUS',
                                                    'Partner country/territory': 'w0',
                                                    'Measurement principle': 'DI',
                                                    'Type of FDI': 'T_FA_F',
                                                    'Type of entity': 'ALL',
                                                    'Accounting entry': 'NET',
                                                    'Level of counterpart': 'IMC',
                                                    'Currency': 'USD'})

        self.assertEqual(data_frame.shape[0], 7)
        self.assertEqual(data_frame.shape[1], 1)

        sname = 'Australia - WORLD - Directional principle: Inward - FDI financial flows - Total - All resident units - Net - Immediate counterpart (Immediate investor or immediate host) - US Dollar - A'

        indx = data_frame.first_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertAlmostEqual(value, 22267.638, 3)

    def test_get_data_from_flat_dataset_with_multi_measures(self):
        """The method is testing load data from flat dataset with with mulitple measures"""

        data_frame = knoema.get('bmlaaaf', **{'Country': 'Albania',
                                              'Borrower': 'Ministry of Finance',
                                              'Guarantor': 'Albania',
                                              'Loan type': 'B loan',
                                              'Loan status': 'EFFECTIVE',
                                              'Currency of commitment': 'eur',
                                              'measure': 'Interest rate'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 1)

        sname = 'Albania - Ministry of Finance - Albania - B LOAN - EFFECTIVE - EUR - Interest Rate - D'
        value = data_frame.get_value('All time', sname)
        self.assertEqual(value, 0.0)

    def test_get_data_from_flat_dataset_without_time(self):
        """The method is testing load data from flat dataset without time"""

        data_frame = knoema.get('pocqwkd', **{'Object type': 'Airports',
                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 1)

        value = data_frame.get_value('All time', 'Airports - Bakel Airport - D')
        self.assertEqual(value, 1.0)

    def test_incorrect_dataset_id(self):
        """The method is testing if dataset id set up incorrectly"""

        with self.assertRaises(ValueError) as context:
            knoema.get('incorrect id', somedim='val1;val2')

        self.assertTrue("Requested dataset doesn't exist or you don't have access to it." in str(context.exception))

    def test_not_all_dims_in_filter(self):
        """The method is testing if dataset id set up incorrectly"""

        with self.assertRaises(ValueError) as context:
            knoema.get('bmlaaaf', **{'Country': 'Albania',
                                     'Borrower': 'Ministry of Finance',
                                     'Guarantor': 'Albania',
                                     'Loan type': 'B loan',
                                     'Loan status': 'EFFECTIVE'})

        self.assertTrue("The following dimension(s) are not set: Currency of Commitment, Measure" in str(context.exception))
