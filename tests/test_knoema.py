"""This is test module for knoema client with test credentials"""

import unittest
import knoema
import pandas

class TestKnoemaClient(unittest.TestCase):
    """This is class with knoema client unit tests with test credentials"""

    base_host = 'knoema.com'

    def setUp(self):
        apicfg = knoema.ApiConfig()
        apicfg.host = self.base_host
        apicfg.app_id = 'FzOYqDg'
        apicfg.app_secret = 'SPrvmY8eGRcGA'

    def test_getdata_singleseries_by_member_id(self):
        """The method is testing getting single series by dimension member ids"""

        data_frame = knoema.get('xmhdwqf', company='c1', indicator='ind_a')
        self.assertEqual(data_frame.shape[0], 11)
        self.assertEqual(data_frame.shape[1], 1)

        self.assertEqual(['Company', 'Indicator', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('BOX', 'Annual', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 85.50)

        indx = data_frame.index[10]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 15.62)

    def test_getdata_multiseries_by_member_id(self):
        """The method is testing getting multiple series by dimension member ids"""

        data_frame = knoema.get('xmhdwqf', company='c1;c2', indicator='ind_m;ind_a')
        self.assertEqual(data_frame.shape[0], 56)
        self.assertEqual(data_frame.shape[1], 4)

        self.assertEqual(['Company', 'Indicator', 'Frequency'], data_frame.columns.names)

        indx = data_frame.index[7]
        sname = ('BOX', 'Monthly', 'M')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23.08)

        indx = data_frame.index[55]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 19.71)

    def test_getdata_multiseries_by_member_name(self):
        """The method is testing getting data by dimension member names"""

        company_names = 'BOX;UBER'
        indicator_names = 'Monthly;Annual'
        data_frame = knoema.get('xmhdwqf', company=company_names, indicator=indicator_names)
        self.assertEqual(data_frame.shape[0], 56)
        self.assertEqual(data_frame.shape[1], 4)

        self.assertEqual(['Company', 'Indicator', 'Frequency'], data_frame.columns.names)

        indx = data_frame.index[7]
        sname = ('BOX', 'Monthly', 'M')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23.08)

        indx = data_frame.index[55]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 19.71)

    def test_getdata_multiseries_by_member_id_range(self):
        """The method is testing getting multiple series by dimension member ids and time range"""
        data_frame = knoema.get('xmhdwqf', company='c1;c2', indicator='ind_a', timerange='2017-2019')

        self.assertEqual(data_frame.shape[0], 11)
        self.assertEqual(data_frame.shape[1], 2)

        indx = data_frame.first_valid_index()
        sname = ('UBER', 'Annual', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 53.03)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 99.15)
   
    def test_getdata_singleseries_difffrequencies_by_member_id(self):
        """The method is testing getting single series on different frequencies by dimension member ids"""

        data_frame = knoema.get('xmhdwqf', company='c1', indicator='ind_multi')
        self.assertEqual(data_frame.shape[1], 3)

        indx = data_frame.first_valid_index()
        sname = ('BOX', 'Multi', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 60.24)

        value = data_frame.at[pandas.to_datetime('2018-01-01'), sname]
        self.assertEqual(value, 80.56)

        indx = data_frame.first_valid_index()
        sname = ('BOX', 'Multi', 'Q')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 47.82)

        value = data_frame.at[pandas.to_datetime('2017-01-01'), sname]
        self.assertEqual(value, 50.28)

    def test_getdata_multiseries_singlefrequency_by_member_id(self):
        """The method is testing getting mulitple series with one frequency by dimension member ids"""

        data_frame = knoema.get('xmhdwqf', company='c2', indicator='ind_multi', frequency='M')
        self.assertEqual(data_frame.shape[1], 1)

        sname = ('UBER', 'Multi', 'M')
        value = data_frame.at[pandas.to_datetime('2017-01-01'), sname]
        self.assertEqual(value, 27.91)

    def test_getdata_multiseries_multifrequency_by_member_id(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimension member ids"""

        data_frame = knoema.get('xmhdwqf', company='c1;c2', indicator='ind_a;ind_multi', frequency='A;M')
        self.assertEqual(data_frame.shape[1], 6)

        sname = ('BOX', 'Annual', 'A')
        value = data_frame.at[pandas.to_datetime('2013-01-01'), sname]
        self.assertEqual(value, 77.93)

    def test_getdata_multiseries_multifrequency_by_member_id_range(self):
        """The method is testing getting mulitple series queriing mulitple frequencies by dimension member ids with time range"""

        data_frame = knoema.get('xmhdwqf', company='c1;c2', indicator='ind_a;ind_multi', frequency='A;M', timerange='2017M1-2020M12')
        self.assertEqual(data_frame.shape[1], 6)

        sname = ('BOX', 'Multi', 'A')
        value = data_frame.at[pandas.to_datetime('2018-01-01'), sname]
        self.assertEqual(value, 80.56)

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
            knoema.get('xmhdwqf', company='c1;c2', indicator='ind_a;ind_multi;ind')

        self.assertTrue('Selection for dimension Indicator contains invalid elements' in str(context.exception))

    def test_wrong_dimension_selection_transform(self):
        """The method is testing if there are incorrect in dimension selection"""

        with self.assertRaises(ValueError) as context:
            knoema.get('xmhdwqf', company='c1;c2', indicator='ind_a;ind_multi;ind', frequency='A', transform='sum')

        self.assertTrue('Selection for dimension Indicator contains invalid elements' in str(context.exception))

    def test_get_data_from_flat_dataset_with_datecolumn(self):
        """The method is testing load data from flat dataset with specifying datecolumn"""

        data_frame = knoema.get('bjxchy', country='Albania', measure='Original Principal Amount ($)', datecolumn='Effective Date (Most Recent)', timerange='2010-2015', frequency='A')
        self.assertEqual(data_frame.shape[0], 5)
        self.assertEqual(data_frame.shape[1], 5)

        sname = ('Albania', 'MINISTRY OF FINANCE', 'Albania', 'FSL', 'Repaying', 'Sum(Original Principal Amount ($))', 'A')
        value = data_frame.at[pandas.to_datetime('2013-01-01'), sname]
        self.assertEqual(value, 40000000.0)

    def test_incorrect_dataset_id(self):
        """The method is testing if dataset id set up incorrectly"""

        with self.assertRaises(ValueError) as context:
            knoema.get('incorrect_id', somedim='val1;val2')

        self.assertTrue("Requested dataset doesn't exist or you don't have access to it." in str(context.exception))

    def test_getdata_multiseries_by_member_key(self):
        """The method is testing getting multiple series by dimension member keys"""

        data_frame = knoema.get('xmhdwqf', company='1000000;1000010', indicator='1000020;1000050')
        self.assertEqual(data_frame.shape[0], 56)
        self.assertEqual(data_frame.shape[1], 4)

        self.assertEqual(['Company', 'Indicator', 'Frequency'], data_frame.columns.names)

        indx = data_frame.index[7]
        sname = ('BOX', 'Monthly', 'M')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23.08)

        indx = data_frame.index[55]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 19.71)

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata_and_mnemomics(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata and mnemonics"""

        data_frame, metadata = knoema.get('eqohmpb', True, **{'Country': '1000000',
                                                    'Indicator': '1000010'})

        self.assertEqual(data_frame.shape[0], 22)
        self.assertEqual(data_frame.shape[1], 6)
        self.assertEqual(metadata.shape[0], 5)
        self.assertEqual(metadata.shape[1], 6)
        self.assertEqual(['Country', 'Indicator', 'Frequency'], data_frame.columns.names)
        self.assertEqual(['Country', 'Indicator', 'Frequency'], metadata.columns.names)

        sname = ('Afghanistan', 'Gross domestic product, current prices', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 0.0)

        indx = metadata.first_valid_index()
        value = metadata.at[indx, sname]
        self.assertAlmostEqual(value, '512')

        self.assertAlmostEqual(metadata.at['Unit', sname], 'Number')
        self.assertAlmostEqual(metadata.at['Scale', sname], 1.0)
        self.assertAlmostEqual(metadata.at['Mnemonics', sname], '512NGDP_A_in_test_dataset')

    def test_weekly_frequency(self):
        """The method is testing load data from regular dataset by weekly frequency"""
   
        data = knoema.get('xmhdwqf', company='BOX', indicator='Weekly', frequency='W')
        sname = ('BOX', 'Weekly', 'W')
        value = data.at[pandas.to_datetime('2018-12-31'), sname]
        self.assertEqual(value, 32.37)
        value = data.at[pandas.to_datetime('2019-12-30'), sname]
        self.assertEqual(value, 83.73)

    def test_incorrect_host_knoema_get(self):
        """The method is negative test on get series from dataset with incorrect host"""

        with self.assertRaises(ValueError) as context:
            apicfg = knoema.ApiConfig()
            apicfg.host = 'knoema_incorect.com'
            _ = knoema.get('IMFWEO2017Apr', country='914', subject='ngdp')
        self.assertTrue("The specified host knoema_incorect.com does not exist" in str(context.exception))
 
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
        data_frame= knoema.get('dzlnsee')
        self.assertEqual(data_frame.shape[1], 4)
        self.assertEqual(['Company', 'Indicator', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('BOX', 'Monthly', 'M')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 23.08)

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
        self.assertEqual(data_frame.shape[0], 60)
        self.assertEqual(data_frame.shape[1], 1)

        self.assertEqual(['Country', 'Series', 'Frequency'], data_frame.columns.names)

        indx = data_frame.first_valid_index()
        sname = ('United States', 'GDP growth (annual %)', 'A')
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2.29999999999968)

        indx = data_frame.index[57]
        value = data_frame.at[indx, sname]
        self.assertEqual(value, 2.99646435222829)


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
        """The method is testing getting data from dataset with columns"""

        data_frame = knoema.get('pqgusj', Company = 'APPLE', columns='*', frequency='FQ')

        self.assertEqual(['Company', 'Indicator', 'Country', 'Frequency', 'Attribute'], data_frame.columns.names)
        self.assertEqual(3, data_frame.columns.size)
        self.assertEqual(('APPLE', 'Export', 'US', 'FQ', 'StatisticalDate'), data_frame.columns.values[1])

    def test_getdata_datelabels(self):
        """The method is testing getting data from dataset with region dimention by region id"""

        data_frame = knoema.get('pqgusj', company = 'APPLE', frequency='FQ')

        self.assertEqual(['Company', 'Indicator', 'Country', 'Frequency'], data_frame.columns.names)
        sname = ('APPLE', 'Export', 'US', 'FQ')

        # this is FQ dateLabel
        value = data_frame.at[pandas.to_datetime('2019-10-09'), sname]

        self.assertEqual(value, 0.531275885712192)

    def test_grouping_functionality(self):
        """The method is tesing grouping functionality"""

        generator = knoema.get('IMFWEO2017Oct', include_metadata = True, group_by='country', country='Albania;Italy;Japan', subject='ngdp', timerange='2005-2010')
        frames = []
        for frame in generator:
            frames.append(frame)

        self.assertEqual(len(frames), 3)
        self.assertIs(type(frames[0].data), pandas.core.frame.DataFrame)
        self.assertIs(type(frames[0].metadata), pandas.core.frame.DataFrame)

    def test_FQ_frequescy(self):
        """Testing FQ frequency"""

        data_frame = knoema.get('xmhdwqf', company='UBER', indicator='Fisqal Quarterly', frequency='FQ')
        self.assertIs(type(data_frame), pandas.core.frame.DataFrame)
        self.assertEqual(data_frame.shape[0], 26)
        self.assertEqual(data_frame.shape[1], 1)

    def test_aggregations(self):
        """Testing aggregations disaggregation"""

        frame = knoema.get('xmhdwqf', company='UBER', indicator='Daily', frequency='D', timerange = '2019M1-2021M4')
        self.assertEqual(frame.shape[0], 366)
        self.assertEqual(frame.shape[1], 1)

        generator = knoema.get('xmhdwqf', group_by = 'company', company='UBER', indicator='Daily', frequency='D', timerange = '2019M1-2021M4')
        for frame in generator:
            self.assertEqual(frame.data.shape[0], 366)
            self.assertEqual(frame.data.shape[1], 1)
            

        frame = knoema.get('xmhdwqf', company='UBER', indicator='Daily', frequency='Q', timerange = '2019M1-2021M4', transform='sum')
        self.assertEqual(frame.shape[0], 5)
        self.assertEqual(frame.shape[1], 1)

        generator = knoema.get('xmhdwqf', group_by='company', company='UBER', indicator='Daily', frequency='Q', timerange = '2019M1-2021M4', transform='sum')
        for frame in generator:
            self.assertEqual(frame.data.shape[0], 5)
            self.assertEqual(frame.data.shape[1], 1)

    def test_auto_aggregations_nodata(self):
        """Testing that auto aggregations returns no data"""

        frame = knoema.get('xmhdwqf', company='UBER', indicator='Daily', frequency='D',
                           timerange='2019M1-2021M4')
        self.assertEqual(frame.shape[0], 366)
        self.assertEqual(frame.shape[1], 1)

        frame = knoema.get('xmhdwqf', company='UBER', indicator='Daily', frequency='Q',
                           timerange='2019M1-2021M4')
        self.assertEqual(frame.shape[0], 0)
        self.assertEqual(frame.shape[1], 0)

    def test_auto_disaggregations_nodata(self):
        """Testing that auto disaggregations returns no data"""

        frame = knoema.get('xmhdwqf', company='UBER', indicator='Annual', frequency='A', timerange='2018-2020')
        self.assertEqual(frame.shape[0], 3)
        self.assertEqual(frame.shape[1], 1)

        frame = knoema.get('xmhdwqf', company='UBER', indicator='Annual', frequency='M', timerange='2018-2020')
        self.assertEqual(frame.shape[0], 0)
        self.assertEqual(frame.shape[1], 0)
