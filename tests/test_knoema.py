"""This is test module for knoema client"""

import unittest
import datetime
import knoema
import urllib
import pandas
import os
import numpy

class TestKnoemaClient(unittest.TestCase):
    """This is class with knoema client unit tests"""

    base_host = 'knoema.com'

    def setUp(self):
        apicfg = knoema.ApiConfig()
        apicfg.host = self.base_host
        apicfg.app_id = os.environ['KNOEMA_APP_ID'] if 'KNOEMA_APP_ID' in os.environ else 'FzOYqDg'
        apicfg.app_secret = os.environ['KNOEMA_APP_SECRET'] if 'KNOEMA_APP_SECRET' in os.environ else 'SPrvmY8eGRcGA'

    def test_getdata_singleseries_by_member_id(self):
        """The method is testing getting single series by dimension member ids"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914', subject='ngdp')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 1)

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('Albania', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 18.489)

        indx = data_frame.index[42]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2292.486)

    def test_getdata_multiseries_by_member_id(self):
        """The method is testing getting multiple series by dimension member ids"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2862.475)

        indx = data_frame.index[42]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23760.331)

    def test_getdata_multiseries_by_member_name(self):
        """The method is testing getting data by dimension member names"""

        subj_names = 'Gross domestic product, current prices (National currency);population (persons)'
        data_frame = knoema.get('IMFWEO2017Apr', country='albania;afghanistan;united states', subject=subj_names)
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2862.475)

        indx = data_frame.index[42]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23760.331)

    def test_getdata_multiseries_by_member_id_range(self):
        """The method is testing getting multiple series by dimension member ids and time range"""
        data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp', timerange='2015-2020')

        self.assertEqual(data_frame.shape[0], 6)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 18036.650)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 22063.044)

    def test_streaming_more_than_1000(self):
        """The method is testing getting multiple series by dimension member ids and time range"""
        data_frame = knoema.get('COMTRADE2015R1', **{'Reporter': 'AFRICA', 'Partner': 'AFRICA', 'Trade Flow': '1', 'Indicator': 'KN.VAL'})
        self.assertEqual(data_frame.shape[0], 31)
        self.assertEqual(data_frame.shape[1], 7884)
   
    def test_getdata_singleseries_difffrequencies_by_member_id(self):
        """The method is testing getting single series on different frequencies by dimension member ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT', subject='BSCI', measure='blsa')
        self.assertEqual(data_frame.shape[1], 2)

        indx = data_frame.first_valid_index()
        sname = ('Austria', 'Confidence indicators', 'Balance; Seasonally adjusted', 'M')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, -5.0)

        value = data_frame.at[datetime.datetime(2017, 5, 1), sname]
        self.assertEqual(value, 2.0)

        indx = data_frame.first_valid_index()
        sname = ('Austria', 'Confidence indicators', 'Balance; Seasonally adjusted', 'Q')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, -5.233333)

        value = data_frame.at[datetime.datetime(2017, 1, 1), sname]
        self.assertEqual(value, 1.566667)

    def test_getdata_multiseries_singlefrequency_by_member_id(self):
        """The method is testing getting mulitple series with one frequency by dimension member ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location=['AT', 'AU'], subject='BSCI', measure='blsa', frequency='Q')
        self.assertEqual(data_frame.shape[1], 2)

        sname = ('Austria', 'Confidence indicators', 'Balance; Seasonally adjusted', 'Q')
        value = data_frame.at[datetime.datetime(2017, 1, 1), sname]
        self.assertEqual(value, 1.566667)

    def test_getdata_multiseries_multifrequency_by_member_id(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimension member ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;AU', subject='BSCI', measure='blsa', frequency='Q;M')
        self.assertEqual(data_frame.shape[1], 4)

        sname = ('Austria', 'Confidence indicators', 'Balance; Seasonally adjusted', 'M')
        value = data_frame.at[datetime.datetime(2017, 3, 1), sname]
        self.assertEqual(value, 2.4)

    def test_getdata_multiseries_multifrequency_by_member_id_range(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimension member ids with time range"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;BE', subject='BSCI', measure='blsa', frequency='Q;M', timerange='2010M1-2015M12')
        self.assertEqual(data_frame.shape[1], 4)

        sname = ('Austria', 'Confidence indicators', 'Balance; Seasonally adjusted', 'M')
        value = data_frame.at[datetime.datetime(2012, 12, 1), sname]
        self.assertEqual(value, -12.4)

    def test_none_dataset(self):
        """The method is testing if dataset set up as None"""

        with self.assertRaises(ValueError) as context:
            knoema.get(None)

        self.assertTrue('Dataset id is not specified' in str(context.exception))       

    def test_wrong_dimension(self):
        """The method is testing if there is wrong dimension name is specified"""
   
        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', indicator='LP;NGDP')

        self.assertTrue('Dimension with id or name indicator is not found' in str(context.exception))

    def test_wrong_dimension_with_transform(self):
        """The method is testing if there is wrong dimension name is specified"""
   
        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', indicator='LP;NGDP', transform='PCH')

        self.assertTrue('Dimension with id or name indicator is not found' in str(context.exception))

    def test_empty_dimension_selection(self):
        """The method is testing if there are no elements in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', country='', subject='LP;NGDP')

        self.assertTrue('Selection for dimension Country is empty' in str(context.exception))

    def test_empty_dimension_selection_with_transform(self):
        """The method is testing if there are no elements in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', country='', subject='LP;NGDP', transform='PCH')

        self.assertTrue('Selection for dimension Country is empty' in str(context.exception))


    def test_wrong_dimension_selection(self):
        """The method is testing if there are incorrect in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', country='914;512;111', subject='LP;N1GDP')

        self.assertTrue('Selection for dimension Subject contains invalid elements' in str(context.exception))

    def test_wrong_dimension_selection_transform(self):
        """The method is testing if there are incorrect in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('IMFWEO2017Apr', country='914;512;111', subject='LP;N1GDP', frequency='A')

        self.assertTrue('Selection for dimension Subject contains invalid elements' in str(context.exception))

    def test_get_data_from_flat_dataset(self):
        """The method is testing load data from flat dataset"""

        data_frame = knoema.get('cblymmf', Country='Albania;Australia', Keyword='FGP;TWP;TRP')
        self.assertEqual(data_frame.shape[0], 32)
        self.assertEqual(data_frame.shape[1], 4)

        self.assertAlmostEqual(float(data_frame.at[30, 'Value']), 98.8368, 4)

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

        sname = ('Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units', 'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
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
        self.assertEqual(data_frame.shape[1], 33)

        value = data_frame.at[0, 'Interest Rate']
        self.assertEqual(value, 0.0)

    def test_get_data_from_flat_dataset_without_time(self):
        """The method is testing load data from flat dataset without time"""

        data_frame = knoema.get('pocqwkd', **{'Object type': 'Airports',
                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 6)

        value = data_frame.at[0, 'Place']
        self.assertEqual(value, 'Bakel')

    def test_get_data_from_flat_dataset_with_datecolumn(self):
        """The method is testing load data from flat dataset with specifying datecolumn"""

        data_frame = knoema.get('bjxchy', country='Albania', measure='Original Principal Amount ($)', datecolumn='Effective Date (Most Recent)', timerange='2010-2015', frequency='A')
        self.assertEqual(data_frame.shape[0], 5)
        self.assertEqual(data_frame.shape[1], 5)

        sname = ('Albania', 'Ministry of Finance', 'Albania', 'FSL', 'Disbursing&Repaying', 'Original Principal Amount ($)', 'A')
        value = data_frame.at['2013-01-01', sname]
        self.assertEqual(value, 40000000.0)

    def test_incorrect_dataset_id(self):
        """The method is testing if dataset id set up incorrectly"""

        with self.assertRaises(ValueError) as context:
            knoema.get('incorrect_id', somedim='val1;val2')

        self.assertTrue("Requested dataset doesn't exist or you don't have access to it." in str(context.exception))

    def test_getdata_multiseries_by_member_key(self):
        """The method is testing getting multiple series by dimension member keys"""

        data_frame = knoema.get('IMFWEO2017Apr', country='1000010;1000000;1001830', subject='1000370;1000040')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2862.475)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23760.331)

    def test_get_data_from_dataset_by_dim_ids(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names by dim ids"""

        data_frame = knoema.get('FDI_FLOW_CTRY', **{'Reporting-country': 'AUS',
                                                    'Partner-country': 'w0',
                                                    'Measurement-principle': 'DI',
                                                    'Type-of-FDI': 'T_FA_F',
                                                    'Type-of-entity': 'ALL',
                                                    'Accounting-entry': 'NET',
                                                    'Level-of-counterpart': 'IMC',
                                                    'Currency': 'USD'})

        self.assertEqual(data_frame.shape[0], 7)
        self.assertEqual(data_frame.shape[1], 1)

        sname = ('Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units', 'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)


    def test_get_data_from_dataset_by_dim_ids_transform(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names by dim ids"""

        data_frame = knoema.get('FDI_FLOW_CTRY', **{'Reporting-country': 'AUS',
                                                    'Partner-country': 'w0',
                                                    'Measurement-principle': 'DI',
                                                    'Type-of-FDI': 'T_FA_F',
                                                    'Type-of-entity': 'ALL',
                                                    'Accounting-entry': 'NET',
                                                    'Level-of-counterpart': 'IMC',
                                                    'Currency': 'USD',
                                                    'Frequency': 'A'})

        self.assertEqual(data_frame.shape[0], 7)
        self.assertEqual(data_frame.shape[1], 1)

        sname = ('Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units', 'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)

    def test_delete_dataset_negative(self):
        """The method is negative test on dataset deletion"""
         
        with self.assertRaises(urllib.error.HTTPError) as context:
            knoema.delete('non_existing_id')
        self.assertTrue('HTTP Error 400: Bad Request' in str(context.exception))

    def test_verify_dataset_negative(self):
        """The method is negative test on dataset verification"""

        with self.assertRaises(ValueError) as context:
            knoema.verify('non_existing_id', datetime.date.today(), 'IMF', 'http://knoema.com/')
        self.assertTrue("Dataset has not been verified, because of the following error(s): Requested dataset doesn't exist or you don't have access to it." in str(context.exception))

    def test_include_metadata_true(self):
        """The method is testing getting multiple series with data and metadata"""

        data, metadata = knoema.get('IMFWEO2017Apr', True, country=['914','512'], subject='lp')
        self.assertEqual(data.shape[0], 43)
        self.assertEqual(data.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], data.columns.names)
        self.assertEqual(metadata.shape[0], 7)
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        indx = data.first_valid_index()
        sname = ('Albania', 'Population (Persons)', 'A')
        value = data.at[indx, sname]
        self.assertEqual(value, 2.762)
        
        indx = metadata.first_valid_index()
        value = metadata.at[indx, sname]
        self.assertEqual(value, '914')

        indx = data.last_valid_index()
        value = data.at[indx, sname]
        self.assertEqual(value, 2.858)

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata"""
  
        data_frame, _ = knoema.get('FDI_FLOW_CTRY', True,**{'Reporting country': 'AUS',
                                                    'Partner country/territory': 'w0',
                                                    'Measurement principle': 'DI',
                                                    'Type of FDI': 'T_FA_F',
                                                    'Type of entity': 'ALL',
                                                    'Accounting entry': 'NET',
                                                    'Level of counterpart': 'IMC',
                                                    'Currency': 'USD'})

        self.assertEqual(data_frame.shape[0], 7)
        self.assertEqual(data_frame.shape[1], 1)

        sname = ('Australia','WORLD','Directional principle: Inward','FDI financial flows - Total','All resident units','Net','Immediate counterpart (Immediate investor or immediate host)','US Dollar','A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)   

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata_transform(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata"""
  
        data_frame, _ = knoema.get('FDI_FLOW_CTRY', True,**{'Reporting country': 'AUS',
                                                    'Partner country/territory': 'w0',
                                                    'Measurement principle': 'DI',
                                                    'Type of FDI': 'T_FA_F',
                                                    'Type of entity': 'ALL',
                                                    'Accounting entry': 'NET',
                                                    'Level of counterpart': 'IMC',
                                                    'Currency': 'USD',
                                                    'Frequency': 'A'})

        self.assertEqual(data_frame.shape[0], 7)
        self.assertEqual(data_frame.shape[1], 1)

        sname = ('Australia','WORLD','Directional principle: Inward','FDI financial flows - Total','All resident units','Net','Immediate counterpart (Immediate investor or immediate host)','US Dollar','A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata_and_mnemomics(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata and mnemonics"""

        data_frame, metadata = knoema.get('xwfebbf', True,**{'Country': '1000000',
                                                    'Indicator': '1000000',
                                                    'Adjustment Type': '1000000',
                                                    'Conversion Type': '1000000'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 1)
        self.assertEqual(metadata.shape[0], 7)
        self.assertEqual(metadata.shape[1], 1)
        self.assertEqual(['Country', 'Indicator', 'Adjustment Type','Conversion Type', 'Frequency'], data_frame.columns.names)
        self.assertEqual(['Country', 'Indicator', 'Adjustment Type','Conversion Type', 'Frequency'], metadata.columns.names)

        sname = ('China','GDP DEFLATOR (% CHANGE, AV)','Not seasonally adjusted', 'Average','A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 2.0)

        indx = metadata.first_valid_index()
        value = metadata.at[indx, sname]
        self.assertAlmostEqual(value, 'CN')

        self.assertAlmostEqual(metadata.at['Unit', sname], 'Unit')
        self.assertAlmostEqual(metadata.at['Scale', sname], 1.0)
        self.assertAlmostEqual(metadata.at['Mnemonics', sname], 'RRRRRRRRR')
       
    def test_get_data_from_flat_dataset_with_multi_measures_and_metadata(self):
        """The method is testing load data from flat dataset with with mulitple measures and metadata"""

        data_frame, metadata = knoema.get('bmlaaaf', True, **{'Country': 'Albania',
                                              'Borrower': 'Ministry of Finance',
                                              'Guarantor': 'Albania',
                                              'Loan type': 'B loan',
                                              'Loan status': 'EFFECTIVE',
                                              'Currency of commitment': 'eur',
                                              'measure': 'Interest rate'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 33)
        self.assertEqual(metadata, None)

        value = data_frame.at[0, 'Undisbursed Amount']
        self.assertEqual(value, 79998000.0)

    def test_get_data_from_flat_dataset_without_time_and_with_metadata(self):
        """The method is testing load data from flat dataset without time and with metadata"""

        data_frame, metadata = knoema.get('pocqwkd',True, **{'Object type': 'Airports',
                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 6)
        self.assertEqual(metadata, None)
   
        self.assertEqual(data_frame.at[0, 'Latitude'],'14.847256')
        self.assertEqual(data_frame.at[0, 'Longitude'],'-12.468264')

    def test_weekly_frequency(self):
        """The method is testing load data from regular dataset by weekly frequency"""
   
        data = knoema.get('WOERDP2015', location='China', Indicator='EMBI Sovereign Spreads (Basis points)', frequency='W')
        sname = ('China', 'EMBI Sovereign Spreads (Basis points)', 'W')
        value = data.at[datetime.datetime(2010, 1, 4), sname]
        self.assertEqual(value, 44)
        value = data.at[datetime.datetime(2015, 9, 7), sname]
        self.assertEqual(value, 183)

    def test_incorrect_host_knoema_get(self):
        """The method is negative test on get series from dataset with incorrect host"""

        with self.assertRaises(ValueError) as context:
            apicfg = knoema.ApiConfig()
            apicfg.host = 'knoema_incorect.com'
            _ = knoema.get('IMFWEO2017Apr', country='914', subject='ngdp')
        self.assertTrue("The specified host knoema_incorect.com does not exist" in str(context.exception))

    def test_incorrect_host_delete_dataset(self):
        """The method is negative test on delete dataset with incorrect host"""

        with self.assertRaises(ValueError) as context:
            apicfg = knoema.ApiConfig()
            apicfg.host = 'knoema_incorect.com'
            knoema.delete('dataset')
        self.assertTrue("The specified host knoema_incorect.com does not exist" in str(context.exception))

    def test_incorrect_host_verify_dataset(self):
        """The method is negative test on verify dataset with incorrect host"""

        with self.assertRaises(ValueError) as context:
            apicfg = knoema.ApiConfig()
            apicfg.host = 'knoema_incorect.com'
            knoema.verify('non_existing_id', datetime.date.today(), 'IMF', 'http://knoema.com')
        self.assertTrue("The specified host knoema_incorect.com does not exist" in str(context.exception))    


    def test_get_data_with_partial_selection(self):
        """The method is testing getting series with partial selection"""

        data_frame = knoema.get('IMFWEO2017Apr', subject = 'flibor6')
        self.assertEqual(data_frame.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('Japan', 'Six-month London interbank offered rate (LIBOR) (Percent)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 10.861)

        indx = data_frame.index[38]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 0.048)

    def test_get_data_with_partial_selection_with_metadata(self):
        """The method is testing getting series with partial selection"""

        _, metadata = knoema.get('IMFWEO2017Apr', True, subject = 'flibor6')
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        sname = ('Japan', 'Six-month London interbank offered rate (LIBOR) (Percent)', 'A')
        self.assertEqual(metadata.at['Country Id',sname],'158')
        self.assertEqual(metadata.at['Subject Id',sname],'FLIBOR6')
        self.assertEqual(metadata.at['Unit',sname],'Percent')
 
    def test_get_data_with_partial_selection_with_metadata_transform(self):
        """The method is testing getting series with partial selection"""

        _, metadata = knoema.get('IMFWEO2017Apr', True, subject = 'flibor6', frequency = 'A')
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        sname = ('Japan', 'Six-month London interbank offered rate (LIBOR) (Percent)', 'A')
        self.assertEqual(metadata.at['Country Id',sname],'158')
        self.assertEqual(metadata.at['Subject Id',sname],'FLIBOR6')
        self.assertEqual(metadata.at['Unit',sname],'Percent')
 
    def test_search_by_mnemonics_data(self):
        """The method is testing searching by mnemonics"""

        data_frame = knoema.get('eqohmpb', mnemonics='512NGDP_A_in_test_dataset')
        self.assertEqual(data_frame.shape[1], 1)
        sname = ('512NGDP_A_in_test_dataset')
        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 0)

    def test_search_by_mnemonics_with_metadata(self):
        """The method is testing searching by mnemonics with metadata"""

        data_frame, metadata = knoema.get('eqohmpb', True, mnemonics='512NGDP_A_in_test_dataset')
        self.assertEqual(data_frame.shape[1], 1)
        sname = ('512NGDP_A_in_test_dataset')
        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 0)
        self.assertEqual(metadata.shape[1], 1)
        self.assertEqual(metadata.shape[0], 5)
        self.assertEqual(metadata.at['Country Id',sname],'512')
        self.assertEqual(metadata.at['Indicator Id',sname],'NGDP')
        self.assertEqual(metadata.at['Unit',sname],'Number')
        self.assertEqual(metadata.at['Mnemonics',sname],'512NGDP_A_in_test_dataset')

    def test_search_by_mnemonics_data_by_all_datasets(self):
        """The method is testing searching by mnemonics by all dataset and returns data"""
        data_frame = knoema.get(mnemonics='512NGDP_A_in_test_dataset;512NGDP')
        self.assertEqual(data_frame.shape[1], 2)
        sname = ('512NGDP_A_in_test_dataset')
        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 0)

    def test_search_by_mnemonics_with_metadata_by_all_datasets(self):
        """The method is testing searching by mnemonics by all dataset and returns data and metadata"""
        data_frame, metadata = knoema.get(None, True, mnemonics='512NGDP_A_in_test_dataset;512NGDP')
        self.assertEqual(data_frame.shape[1], 2)
        sname = ('512NGDP_A_in_test_dataset')
        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 0)
        self.assertEqual(metadata.at['Country Id',sname],'512')
        self.assertEqual(metadata.at['Indicator Id',sname],'NGDP')
        self.assertEqual(metadata.at['Unit',sname],'Number')
        self.assertEqual(metadata.at['Mnemonics',sname],'512NGDP_A_in_test_dataset')

    def test_search_by_mnemonics_with_metadata_by_all_datasets_transform(self):
        """The method is testing searching by mnemonics by all dataset and returns data and metadata"""
        data_frame, metadata = knoema.get(None, True, mnemonics='512NGDP_R;512NGDP', transform='PCH', frequency='A')
        self.assertEqual(data_frame.shape[1], 2)
        sname = ('512NGDP_R')
        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 8.69275111845198)
        self.assertEqual(metadata.at['Country Id',sname],'512')
        self.assertEqual(metadata.at['Indicator Id',sname],'NGDP_R')
        self.assertEqual(metadata.at['Unit',sname],'%')
        self.assertEqual(metadata.at['Mnemonics',sname],'512NGDP_R')
    
    def test_get_all_series_from_dataset(self):
        """The method is testing getting all series from dataset"""
        data_frame= knoema.get('eqohmpb')
        self.assertEqual(data_frame.shape[1], 14)
        self.assertEqual(['Country', 'Indicator', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('Afghanistan', 'Gross domestic product, current prices', 'M')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 26.02858277)

    def test_with_empty_attributes_in_the_dimensions(self):
        data, metadata = knoema.get('IMFWEO2017Oct', include_metadata=True, country=['914','512'], subject='lp')
        self.assertEqual(data.shape[0], 43)
        self.assertEqual(data.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], data.columns.names)
        self.assertEqual(metadata.shape[0], 8)
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        indx = data.first_valid_index()
        sname = ('Albania', 'Population (Persons)', 'A')
        value = data.at[indx, sname]
        self.assertEqual(value, 2.762)
        
        self.assertEqual(metadata.at['Country Id',sname],'914')
        self.assertEqual(metadata.at['Subject Id',sname],'LP')
        self.assertEqual(metadata.at['Subject SubjectDescription',sname],None)
        self.assertEqual(metadata.at['Subject SubjectNotes',sname],None)
        self.assertEqual(metadata.at['Subject Relevant',sname],None)
        self.assertEqual(metadata.at['Unit',sname],'Persons (Millions)')
        self.assertEqual(metadata.at['Mnemonics',sname],None)

        indx = data.last_valid_index()
        value = data.at[indx, sname]
        self.assertEqual(value, 2.856)
        
    def test_with_empty_attributes_in_the_dimensions_transform(self):
        data, metadata = knoema.get('IMFWEO2017Oct', include_metadata=True, country=['914','512'], subject='lp', frequency='A')
        self.assertEqual(data.shape[0], 43)
        self.assertEqual(data.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], data.columns.names)
        self.assertEqual(metadata.shape[0], 8)
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        indx = data.first_valid_index()
        sname = ('Albania', 'Population (Persons)', 'A')
        value = data.at[indx, sname]
        self.assertEqual(value, 2.762)
        
        self.assertEqual(metadata.at['Country Id',sname],'914')
        self.assertEqual(metadata.at['Subject Id',sname],'LP')
        self.assertEqual(metadata.at['Subject SubjectDescription',sname],None)
        self.assertEqual(metadata.at['Subject SubjectNotes',sname],None)
        self.assertEqual(metadata.at['Subject Relevant',sname],None)
        self.assertEqual(metadata.at['Unit',sname],'Persons (Millions)')
        self.assertEqual(metadata.at['Mnemonics',sname],None)

        indx = data.last_valid_index()
        value = data.at[indx, sname]
        self.assertEqual(value, 2.856)

        
    def test_getdata_from_private_community(self):
        """The method is testing getting data from private community"""

        apicfgCommunity = knoema.ApiConfig()
        apicfgCommunity.host = 'teryllol.' + self.base_host
        apicfgCommunity.app_id = 's81oiSY'
        apicfgCommunity.app_secret='g4lKmIOPE2R4w'

        data_frame = knoema.get('qfsneof', country='USA', series='NY.GDP.MKTP.KD.ZG')
        self.assertEqual(data_frame.shape[0], 59)
        self.assertEqual(data_frame.shape[1], 1)

        self.assertEqual(['Country', 'Series', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'GDP growth (annual %)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2.29999999999968)

        indx = data_frame.index[57]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2.92732272821085)


    def test_getdata_custom_separator(self):
        """The method is testing getting data with custom separator"""

        data_frame = knoema.get('IMFWEO2019Oct',
            country='Albania',
            subject='Gross domestic product, constant prices (Percent change)|Gross domestic product per capita, constant prices (Purchasing power parity; 2011 international dollar)',
            separator='|')
        self.assertEqual(data_frame.shape[0], 45)
        self.assertEqual(data_frame.shape[1], 2)

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('Albania', 'Gross domestic product per capita, constant prices (Purchasing power parity; 2011 international dollar)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 4832.599)

    def test_getdata_default_web_separator(self):
        """The method is testing getting data with custom separator"""

        data_frame = knoema.get('IMFWEO2019Oct',
            country='Albania',
            subject='NGDP_RPCH,NGDPRPPPPC',
            separator=',')
        self.assertEqual(data_frame.shape[0], 45)
        self.assertEqual(data_frame.shape[1], 2)

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('Albania', 'Gross domestic product per capita, constant prices (Purchasing power parity; 2011 international dollar)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 4832.599)

    def test_getdata_region_dim_region_id(self):
        """The method is testing getting data from dataset with region dimention by region id"""

        data_frame = knoema.get('IMFWEO2019Oct',
            country = 'AF',
            subject = ['Gross domestic product, constant prices (Percent change)']
        )

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)
        self.assertEqual('Afghanistan', data_frame.columns.values[0][0])

        indx = data_frame.first_valid_index()
        sname = ('Afghanistan', 'Gross domestic product, constant prices (Percent change)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 8.692)

    def test_getdata_region_as_dim_region_id(self):
        """The method is testing getting data from dataset with region dimention by region id"""

        data_frame = knoema.get('IMFWEO2019Oct',
            region = 'AF',
            subject = ['Gross domestic product, constant prices (Percent change)']
        )

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)
        self.assertEqual('Afghanistan', data_frame.columns.values[0][0])

        indx = data_frame.first_valid_index()
        sname = ('Afghanistan', 'Gross domestic product, constant prices (Percent change)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 8.692)

    def test_getdata_with_columns(self):
        """The method is testing getting data from dataset with region dimention by region id"""

        data_frame = knoema.get('ERM', Company = '147', columns='*')

        self.assertEqual(['Company', 'Kpi Type', 'Kpi', 'Frequency', 'Attribute'], data_frame.columns.names)
        self.assertEqual(4, data_frame.columns.size)
        self.assertEqual(('Shopify, Inc. Class A', 'Reported Metric', 'GPV', 'FQ', 'StatisticalDate'), data_frame.columns.values[1])

    def test_getdata_datelabels(self):
        """The method is testing getting data from dataset with region dimention by region id"""

        data_frame = knoema.get('1010DDCDYOY072020', Company = '147')

        self.assertEqual(['Category', 'Company', 'Country', 'Indicator', 'Frequency'], data_frame.columns.names)
        sname = ('Shopify', 'SHOPIFY', 'US', 'Debit Avg Customer Spend YoY', 'FQ')

        # this is FQ dateLabel
        value = data_frame.at[datetime.datetime(2013, 3, 31), sname]

        self.assertEqual(value, -0.1537901182)

    def test_search_wrapper_search_for_timeseries(self):
        """The method is testing search wrapper to search for timeseries"""

        search_results = knoema.search('Italy inflation')
        search_results_1 = knoema.search('UAE oil production')

        self.assertTrue(len(search_results.series) > 0)
        self.assertTrue(len(search_results_1.series) > 0)

        first_series = search_results.series[0].get()
        first_series_1 = search_results_1.series[0].get()

        self.assertIs(type(first_series), pandas.core.frame.DataFrame)
        self.assertIs(type(first_series_1), pandas.core.frame.DataFrame)

    def test_grouping_functionality(self):
        """The method is tesing grouping functionality"""

        generator = knoema.get('IMFWEO2017Oct', include_metadata = True, group_by='country', country='Albania;Italy;Japan', subject='ngdp', timerange='2005-2010')
        frames = []
        for frame in generator:
            frames.append(frame)

        self.assertEqual(len(frames), 3)
        self.assertIs(type(frames[0].data), pandas.core.frame.DataFrame)
        self.assertIs(type(frames[0].metadata), pandas.core.frame.DataFrame)

    def test_too_long_get_query_string(self):
        """The method is testing issue with loo long get query string"""

        for frame in knoema.get('US500COMPFINST2017Oct', include_metadata = True, indicator = 'Total Assets', frequency = 'A', group_by = 'company'):
            company = frame.id
            data_frame = frame.data
            metadata_frame = frame.metadata

            self.assertIsNotNone(company, None)
            self.assertIs(type(data_frame), pandas.core.frame.DataFrame)
            self.assertIs(type(metadata_frame), pandas.core.frame.DataFrame)

            break

    def test_too_long_data_url(self):
        """The method is testing issue with loo long get query string"""
        subject = ['Gross domestic product, constant prices (Percent change)',
            'Gross domestic product, constant prices (Percent change (market exchange rates))',
            'Gross domestic product, current prices (U.S. dollars)',
            'Rubber, No.1 Rubber Smoked Sheet, FOB Maylaysian/Singapore, US cents per pound (U.S. cents)',
            'Gross domestic product, current prices (Purchasing power parity; international dollars)',
            'Gross national savings (Percent of GDP)',
            'Total investment (Percent of GDP)',
            'Inflation, average consumer prices (Percent change)',
            'Inflation, end of period consumer prices (Percent change)',
            'Trade volume of goods and services (Percent change)',
            'Volume of imports of goods and services (Percent change)',
            'Volume of Imports of goods (Percent change)',
            'Volume of exports of goods and services (Percent change)',
            'Volume of exports of goods (Percent change)',
            'Current account balance (U.S. dollars)',
            'Commodity Price Index includes both Fuel and Non-Fuel Price Indices (Index, 2005=100)',
            'Commodity Non-Fuel Price Index includes Food and Beverages and Industrial Inputs Price Indices (Index, 2005=100)',
            'Commodity Industrial Inputs Price Index includes Agricultural Raw Materials and Metals Price Indices (Index, 2005=100)',
            'Coal, Australian thermal coal, 1200- btu/pound, less than 1% sulfur, 14% ash, FOB Newcastle/Port Kembla, US$ per metric tonne (U.S. dollars)',
            'Coal, South African export price, US$ per metric tonne (U.S. dollars)',
            'Commodity Coal Price Index includes Australian and South African Coal (Index, 2005=100)',
            'Commodity Fuel (energy) Index includes Crude oil (petroleum), Natural Gas, and Coal Price Indices (Index, 2005=100)',
            'Commodity Natural Gas Price Index includes European, Japanese, and American Natural Gas Price Indices (Index, 2005=100)',
            'Crude Oil (petroleum),  Dated Brent, light blend 38 API, fob U.K., US$ per barrel (U.S. dollars)',
            'Crude Oil (petroleum), Price index simple average of three spot prices (APSP); Dated Brent, West Texas Intermediate, and the Dubai Fateh (Index, 2005=100)',
            'Coffee, Robusta, International Coffee Organization New York cash price, ex-dock New York, US cents per pound (U.S. cents)',
            'Commodity Beverage Price Index includes Coffee, Tea, and Cocoa (Index, 2005=100)',
            'Commodity Cereals Price Index includes Wheat, Maize (Corn), Rice, and Barley (Index, 2005=100)',
            'Commodity Coffee Price Index includes Other Mild Arabicas and Robusta (Index, 2005=100)']
        separator = ";;"
        frame = knoema.get("IMFWEO2021Apr", country="World", separator=separator, subject=separator.join(subject))
        self.assertEqual(frame.shape[1], len(subject))

    def test_ticker_endpoint(self):
        """Testing ticker endpoint"""

        ticker = knoema.ticker('DDD')

        self.assertEqual(ticker.name, '3D Systems Corporation')
        self.assertEqual(len(ticker.groups), 4)
        self.assertEqual(ticker.groups[0].name, 'HOLT Scorecards')
        self.assertEqual(len(ticker.groups[0].indicators), 23)
        self.assertEqual(ticker.groups[0].indicators[0].name, '% Change in CFROI')
        self.assertIs(type(ticker.groups[0].indicators[0].get()), pandas.core.frame.DataFrame)

        indicator = ticker.get_indicator('% Upside / Downside')
        self.assertEqual(indicator.name, '% Upside / Downside')
        self.assertIs(type(indicator.get()), pandas.core.frame.DataFrame)

    def test_FQ_frequescy(self):
        """Testing FQ frequency"""

        data_frame = knoema.get('1010DDCDYOY', company='GRUBHUB', category='Eat24', indicator='Debit Avg Customer Spend YoY', frequency='FQ')
        self.assertIs(type(data_frame), pandas.core.frame.DataFrame)
        self.assertEqual(data_frame.shape[0], 30)
        self.assertEqual(data_frame.shape[1], 1)

    def test_aggregations(self):
        """Testing aggregations disaggregation"""

        frame = knoema.get('SDRDPCRBYP', company = 'ACTIVISION', indicator = 'Digital Premium Console Revenue', publisher = 'Activision Blizzard, Inc.', frequency = 'D', timerange = '2018M1-2020M4')
        self.assertEqual(frame.shape[0], 851)
        self.assertEqual(frame.shape[1], 1)

        generator = knoema.get('SDRDPCRBYP', group_by = 'company', company = 'ACTIVISION', indicator = 'Digital Premium Console Revenue', publisher = 'Activision Blizzard, Inc.', frequency = 'D', timerange = '2018M1-2020M4')
        for frame in generator:
            self.assertEqual(frame.data.shape[0], 851)
            self.assertEqual(frame.data.shape[1], 1)
            

        frame = knoema.get('SDRDPCRBYP', company = 'ACTIVISION', indicator = 'Digital Premium Console Revenue', publisher = 'Activision Blizzard, Inc.', frequency = 'Q', timerange = '2018M1-2020M4')
        self.assertEqual(frame.shape[0], 10)
        self.assertEqual(frame.shape[1], 1)

        generator = knoema.get('SDRDPCRBYP', group_by = 'company', company = 'ACTIVISION', indicator = 'Digital Premium Console Revenue', publisher = 'Activision Blizzard, Inc.', frequency = 'Q', timerange = '2018M1-2020M4')
        for frame in generator:
            self.assertEqual(frame.data.shape[0], 10)
            self.assertEqual(frame.data.shape[1], 1)

    def test_noaggregation(self):
        data = knoema.get(**{"dataset" : "IMFWEO2020Oct", "country": "United States", "subject": "Population (Persons)", "frequency" : "Q" , "transform": 'NOAGG'})
        self.assertEqual(data.shape[1], 0)

    def test_upload_generated_frames(self):
        tuples = list(zip(*[['bar', 'bar', 'baz', 'baz', 'foo', 'foo', 'qux', 'qux'], ['one', 'two', 'one', 'two', 'one', 'two', 'one', 'two']]))
        index = pandas.MultiIndex.from_tuples(tuples, names=['first', 'second'])
        dates = pandas.date_range('1/1/2000', periods=8)
        frame = pandas.DataFrame(numpy.random.randn(8, 8), index=dates, columns=index)
        res = knoema.upload(frame, name = 'Test dataset')
        self.assertIs(type(res), str)
        self.assertEqual(len(res), 7)

        frame = pandas.DataFrame(numpy.random.randn(8, 4), index=dates, columns=['A', 'B', 'C', 'D'])
        res = knoema.upload(frame, name = 'Test dataset')
        self.assertIs(type(res), str)
        self.assertEqual(len(res), 7)

    def test_upload_frames_from_existing_datasets(self):
        frame = knoema.get('IMFWEO2017Oct', country='Albania;United States;Italy', subject='ngdp', timerange = '2015-2020')
        res = knoema.upload(frame, name = 'Test dataset')

        self.assertIs(type(res), str)
        self.assertEqual(len(res), 7)
