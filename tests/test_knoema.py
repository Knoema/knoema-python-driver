"""This is test module for knoema client"""

import unittest
import datetime
import knoema
import urllib

class TestKnoemaClient(unittest.TestCase):
    """This is class with knoema client unit tests"""

    def test_getdata_singleseries_by_member_id(self):
        """The method is testing getting single series by dimension member ids"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914', subject='ngdp')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 1)

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('Albania', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 18.489)

        indx = data_frame.index[42]
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2292.486)

    def test_getdata_multiseries_by_member_id(self):
        """The method is testing getting multiple series by dimension member ids"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        self.assertEqual(['Country', 'Subject', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2862.475)

        indx = data_frame.index[42]
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 23760.331)

    def test_getdata_multiseries_by_member_name(self):
        """The method is testing getting data by dimension member names"""

        subj_names = 'Gross domestic product, current prices (National currency);population (persons)'
        data_frame = knoema.get('IMFWEO2017Apr', country='albania;afghanistan;united states', subject=subj_names)
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2862.475)

        indx = data_frame.index[42]
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 23760.331)

    def test_getdata_multiseries_by_member_id_range(self):
        """The method is testing getting multiple series by dimension member ids and time range"""

        data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp', timerange='2015-2020')
        self.assertEqual(data_frame.shape[0], 6)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 18036.650)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 22063.044)

    def test_getdata_singleseries_difffrequencies_by_member_id(self):
        """The method is testing getting single series on different frequencies by dimension member ids"""
        
        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT', subject='BSCI', measure='blsa')
        self.assertEqual(data_frame.shape[1], 2)

        indx = data_frame.first_valid_index()
        sname = ('Austria', 'Confidence indicators', 'Balance, s.a.', 'M')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, -5.0)

        value = data_frame.get_value(datetime.datetime(2017, 5, 1), sname)
        self.assertEqual(value, 2.0)

        indx = data_frame.first_valid_index()
        sname = ('Austria', 'Confidence indicators', 'Balance, s.a.', 'Q')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, -5.233333)

        value = data_frame.get_value(datetime.datetime(2017, 1, 1), sname)
        self.assertEqual(value, 1.566667)

    def test_getdata_multiseries_singlefrequency_by_member_id(self):
        """The method is testing getting mulitple series with one frequency by dimension member ids"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location=['AT', 'AU'], subject='BSCI', measure='blsa', frequency='Q')
        self.assertEqual(data_frame.shape[0], 205)
        self.assertEqual(data_frame.shape[1], 2)

        sname = ('Austria', 'Confidence indicators', 'Balance, s.a.', 'Q')
        value = data_frame.get_value(datetime.datetime(2017, 1, 1), sname)
        self.assertEqual(value, 1.566667)

    def test_getdata_multiseries_multifrequency_by_member_id(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimension member ids"""
        
        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;AU', subject='BSCI', measure='blsa', frequency='Q;M')
        self.assertEqual(data_frame.shape[1], 3)

        sname = ('Austria', 'Confidence indicators', 'Balance, s.a.', 'M')
        value = data_frame.get_value(datetime.datetime(2017, 3, 1), sname)
        self.assertEqual(value, 2.4)

    def test_getdata_multiseries_multifrequency_by_member_id_range(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimension member ids with time range"""

        data_frame = knoema.get('MEI_BTS_COS_2015', location='AT;BE', subject='BSCI', measure='blsa', frequency='Q;M', timerange='2010M1-2015M12')
        self.assertEqual(data_frame.shape[1], 3)

        sname = ('Austria', 'Confidence indicators', 'Balance, s.a.', 'M')
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

        self.assertTrue('Dimension with id or name indicator is not found' in str(context.exception))

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

        sname = ('Albania', 'FGP', 'D')
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

        sname = ('Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units', 'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

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

        sname = ('Albania', 'Ministry of Finance', 'Albania', 'B LOAN', 'EFFECTIVE', 'EUR', 'Interest Rate', 'D')
        value = data_frame.get_value('All time', sname)
        self.assertEqual(value, 0.0)

    def test_get_data_from_flat_dataset_without_time(self):
        """The method is testing load data from flat dataset without time"""

        data_frame = knoema.get('pocqwkd', **{'Object type': 'Airports',
                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 1)

        sname = ('Airports', 'Bakel Airport', 'D')
        value = data_frame.get_value('All time', sname)
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

        self.assertTrue('The following dimension(s) are not set: Currency of Commitment, Measure' in str(context.exception))

    def test_getdata_multiseries_by_member_key(self):
        """The method is testing getting multiple series by dimension member keys"""

        data_frame = knoema.get('IMFWEO2017Apr', country='1000010;1000000;1001830', subject='1000370;1000040')
        self.assertEqual(data_frame.shape[0], 43)
        self.assertEqual(data_frame.shape[1], 6)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'Gross domestic product, current prices (National currency)', 'A')
        value = data_frame.get_value(indx, sname)
        self.assertEqual(value, 2862.475)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
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
        value = data_frame.get_value(indx, sname)
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
        self.assertAlmostEqual(value, 22267.638, 3)

    def test_delete_dataset_negative(self):
        """The method is negative test on dataset deletion"""
           
        with self.assertRaises(urllib.error.HTTPError) as context:
            knoema.delete('non_existing_id')
        self.assertTrue('HTTP Error 400: Bad Request' in str(context.exception))

    def test_verify_dataset_negative(self):
        """The method is negative test on dataset verification"""

        with self.assertRaises(ValueError) as context:
            knoema.verify('non_existing_id', datetime.date.today(), 'IMF', 'http://knoema.gic.com.sg/')
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
        value = data.get_value(indx, sname)
        self.assertEqual(value, 2.762)
        
        indx = metadata.first_valid_index()
        value = metadata.get_value(indx, sname)
        self.assertEqual(value, '914')

        indx = data.last_valid_index()
        value = data.get_value(indx, sname)
        self.assertEqual(value, 2.858)

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata"""

        data_frame, metadata = knoema.get('FDI_FLOW_CTRY', True,**{'Reporting country': 'AUS',
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
        value = data_frame.get_value(indx, sname)
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.get_value(indx, sname)
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
        value = data_frame.get_value(indx, sname)
        self.assertAlmostEqual(value, 2.0)

        indx = metadata.first_valid_index()
        value = metadata.get_value(indx, sname)
        self.assertAlmostEqual(value, 'CN')

        self.assertAlmostEqual(metadata.get_value('Unit', sname), 'Unit')
        self.assertAlmostEqual(metadata.get_value('Scale', sname), 1.0)
        self.assertAlmostEqual(metadata.get_value('Mnemonics', sname), 'RRRRRRRRR')

    def test_get_data_from_dataset_big_selection(self):
        """The method is testing load data from regular dataset with big selection"""
        
        with self.assertRaises(ValueError) as context:
            data = knoema.get('IMFWEO2017Apr', frequency='A', Country='512;914;612;614;311;213;911;193;122;912;313;419;513;316;913;124;339;638;514;218;963;616;223;516;918;748;618;624;522;622;156;626;628;228;924;233;632;636;634;238;662;960;423;935;128', Subject='NGDP_RPCH;NGDP_RPCHMK;NGDPD;NGDP;NGDP_R;NGDP_D;NGDP_FY;PPPGDP;PPPPC;PPPSH;NGDPDPC;NGDPPC;NGDPRPC;NGSD_NGDP;NID_NGDP;PPPEX;NGAP_NPGDP;PCPIPCH;PCPIEPCH;PCPI;PCPIE;FLIBOR3;FLIBOR6;TRADEPCH;TM_RPCH;TMG_RPCH;TX_RPCH;TXG_RPCH;TTPCH;TTTPCH;TXGM_D;TXGM_DPCH;LP;LE;LUR')
        self.assertTrue("Underlying data is very large. Can't create visualization." in str(context.exception))   
       
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
        self.assertEqual(data_frame.shape[1], 1)
        self.assertEqual(metadata.shape[0], 6)
        self.assertEqual(metadata.shape[1], 1)

        sname = ('Albania', 'Ministry of Finance', 'Albania', 'B LOAN', 'EFFECTIVE', 'EUR', 'Interest Rate', 'D')
        value = data_frame.get_value('All time', sname)
        self.assertEqual(value, 0.0)
        self.assertEqual(metadata.get_value('Country Country Code',sname),'AL')
        self.assertEqual(metadata.get_value('Scale',sname),1)
        self.assertEqual(metadata.get_value('Unit',sname),'None')


    def test_get_data_from_flat_dataset_without_time_and_with_metadata(self):
        """The method is testing load data from flat dataset without time and with metadata"""
        data_frame, metadata = knoema.get('pocqwkd',True, **{'Object type': 'Airports',
                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 1)
        self.assertEqual(metadata.shape[0], 5)
        self.assertEqual(metadata.shape[1], 1)

        sname = ('Airports', 'Bakel Airport', 'D')
        value = data_frame.get_value('All time', sname)
        self.assertEqual(value, 1.0)   
        self.assertEqual(metadata.get_value('Object Name Latitude',sname),'14.847256')
        self.assertEqual(metadata.get_value('Object Name Longitude',sname),'-12.468264')

        self.assertEqual(metadata.get_value('Scale',sname),1)
        self.assertEqual(metadata.get_value('Unit',sname),'# of records')
