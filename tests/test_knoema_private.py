"""This is test module for knoema client"""
"""Works only with special credentials"""

import unittest
import knoema
import pandas
import os

class TestKnoemaClient(unittest.TestCase):
    """This is class with knoema client unit tests"""

    base_host = 'knoema.com'

    def setUp(self):
        apicfg = knoema.ApiConfig()
        apicfg.host = os.environ['BASE_HOST'] if 'BASE_HOST' in os.environ else self.base_host
        apicfg.app_id = os.environ['KNOEMA_APP_ID'] if 'KNOEMA_APP_ID' in os.environ else ''
        apicfg.app_secret = os.environ['KNOEMA_APP_SECRET'] if 'KNOEMA_APP_SECRET' in os.environ else ''

    def test_too_long_get_query_string(self):
        """The method is testing issue with loo long get query string"""

        for frame in knoema.get('US500COMPFINST2017Oct', include_metadata=True, indicator='Total Assets',
                                frequency='A', group_by='company'):
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
        self.assertEqual(ticker.groups[0].name, 'Credit Risk')
        self.assertEqual(len(ticker.groups[0].indicators), 6)
        self.assertEqual(ticker.groups[0].indicators[0].name, 'Bankruptcy Base FRISK Probability')
        self.assertIs(type(ticker.groups[0].indicators[0].get()), pandas.core.frame.DataFrame)

        indicator = ticker.get_indicator('Bankruptcy Base FRISK Probability')
        self.assertEqual(indicator.name, 'Bankruptcy Base FRISK Probability')
        self.assertIs(type(indicator.get()), pandas.core.frame.DataFrame)

    def test_streaming_more_than_1000(self):
        """The method is testing getting multiple series by dimension member ids and time range"""
        data_frame = knoema.get('nama_r_e3gdp', **{'Measure': 'Euro per inhabitant'})
        self.assertEqual(data_frame.shape[0], 12)
        self.assertEqual(data_frame.shape[1], 1738)

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

    def test_noaggregation(self):
        data = knoema.get(**{"dataset" : "IMFWEO2020Oct", "country": "United States", "subject": "Population (Persons)", "frequency" : "Q" , "transform": 'NOAGG'})
        self.assertEqual(data.shape[1], 0)

    def test_include_metadata_true(self):
        """The method is testing getting multiple series with data and metadata"""

        data, metadata = knoema.get('IMFWEO2017Apr', True, country=['914', '512'], subject='lp')
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

    def test_get_data_with_partial_selection_with_metadata_transform(self):
        """The method is testing getting series with partial selection"""

        _, metadata = knoema.get('IMFWEO2017Apr', True, subject='flibor6', frequency='A')
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        sname = ('Japan', 'Six-month London interbank offered rate (LIBOR) (Percent)', 'A')
        self.assertEqual(metadata.at['Country Id', sname], '158')
        self.assertEqual(metadata.at['Subject Id', sname], 'FLIBOR6')
        self.assertEqual(metadata.at['Unit', sname], 'Percent')


    def test_get_data_with_partial_selection_with_metadata(self):
        """The method is testing getting series with partial selection"""

        _, metadata = knoema.get('IMFWEO2017Apr', True, subject = 'flibor6')
        self.assertEqual(metadata.shape[1], 2)
        self.assertEqual(['Country', 'Subject', 'Frequency'], metadata.columns.names)
        sname = ('Japan', 'Six-month London interbank offered rate (LIBOR) (Percent)', 'A')
        self.assertEqual(metadata.at['Country Id',sname],'158')
        self.assertEqual(metadata.at['Subject Id',sname],'FLIBOR6')
        self.assertEqual(metadata.at['Unit',sname],'Percent')


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

    def test_get_data_from_flat_dataset_without_time_and_with_metadata(self):
        """The method is testing load data from flat dataset without time and with metadata"""

        data_frame, metadata = knoema.get('pocqwkd', True, **{'Object type': 'Airports',
                                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 6)
        self.assertEqual(metadata, None)

        self.assertEqual(data_frame.at[0, 'Latitude'], '14.847256')
        self.assertEqual(data_frame.at[0, 'Longitude'], '-12.468264')


    def test_get_data_from_flat_dataset_without_time(self):
        """The method is testing load data from flat dataset without time"""

        data_frame = knoema.get('pocqwkd', **{'Object type': 'Airports',
                                              'Object name': 'Bakel airport'})

        self.assertEqual(data_frame.shape[0], 1)
        self.assertEqual(data_frame.shape[1], 6)

        value = data_frame.at[0, 'Place']
        self.assertEqual(value, 'Bakel')

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

    def test_get_data_from_flat_dataset(self):
        """The method is testing load data from flat dataset"""

        data_frame = knoema.get('cblymmf', Country='Albania;Australia', Keyword='FGP;TWP;TRP')
        self.assertEqual(data_frame.shape[0], 32)
        self.assertEqual(data_frame.shape[1], 4)

        self.assertAlmostEqual(float(data_frame.at[30, 'Value']), 98.8368, 4)

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata_transform(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata"""

        data_frame, _ = knoema.get('FDI_FLOW_CTRY', True, **{'Reporting country': 'AUS',
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

        sname = (
        'Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units',
        'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)

    def test_get_data_from_dataset_with_multiword_dimnames_and_metadata(self):
        """The method is testing load data from regular dataset with dimenions that have multi word names include metadata"""

        data_frame, _ = knoema.get('FDI_FLOW_CTRY', True, **{'Reporting country': 'AUS',
                                                             'Partner country/territory': 'w0',
                                                             'Measurement principle': 'DI',
                                                             'Type of FDI': 'T_FA_F',
                                                             'Type of entity': 'ALL',
                                                             'Accounting entry': 'NET',
                                                             'Level of counterpart': 'IMC',
                                                             'Currency': 'USD'})

        self.assertEqual(data_frame.shape[0], 7)
        self.assertEqual(data_frame.shape[1], 1)

        sname = (
        'Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units',
        'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)

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

        sname = (
        'Australia', 'WORLD', 'Directional principle: Inward', 'FDI financial flows - Total', 'All resident units',
        'Net', 'Immediate counterpart (Immediate investor or immediate host)', 'US Dollar', 'A')

        indx = data_frame.first_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 31666.667, 3)

        indx = data_frame.last_valid_index()
        value = data_frame.at[indx, sname]
        self.assertAlmostEqual(value, 22267.638, 3)