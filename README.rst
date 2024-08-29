========================================
Official Python package for Knoema's API
========================================

This is the official documentation for Knoema's Python Package. The package can be used for obtaining data from the datasets from the site knoema.com and uploading data to datasets. This package is compatible with Python v3.9+.

************
Installation
************

The installation process varies depending on your python version and system used. However, in most cases, the following should work::

        pip install knoema 

Alternatively, on some systems, python3 may use a different pip executable and may need to be installed via an alternate pip command. For example::

        pip3 install knoema
                
*************
Configuration
*************
Authorization is needed to increase the number of requests permitted and to access data that is not publicly available to all users within the platform. The application created under your account shares the same access rights through the API as you enjoy through registered access to your Knoema system.

Step 1: Get App ID and App Secret. Log into a Knoema system (the host), open the link {host}/user/apps, click Create New, give your app a name, and click Save. 
 
Step 2: In python, authenticate using the following code, where Host is the Knoema system (e.g., knoema.com) you used to generate the App ID and App Secret in Step 1::

    import knoema
    apicfg = knoema.ApiConfig()
    apicfg.host = 'Host'
    apicfg.app_id = 'App ID'
    apicfg.app_secret = 'App Secret'

*******************************
Retrieving series from datasets
*******************************
There are one method for retrieving series from datasets in Python: the **get** method. The method works with knoema datasets.

The following quick call can be used to retrieve a time-series from dataset (three cases with the same result)::

   import knoema
   data_frame = knoema.get('IMFWEO2017Oct', country='Albania', subject='Gross domestic product, current prices (U.S. dollars)')
   data_frame = knoema.get('IMFWEO2017Oct', country='AL', subject='Gross domestic product, current prices (U.S. dollars)')
   data_frame = knoema.get('IMFWEO2017Oct', region='AL', subject='Gross domestic product, current prices (U.S. dollars)')

where:

* 'IMFWEO2017Oct' this is a dataset ID. Dataset ID can be found from the URL, if you open any dataset you will see it right after the hostname: https://knoema.com/IMFWEO2017Oct/imf-world-economic-outlook-weo-database-october-2017.
* country and subject are dimensions names
* AL is region id for Albania
* region is synonymous for name of geographic dimension

This example finds all data points for the dataset IMFWEO2017Oct with selection by country = *Albania* and subject =  *Gross domestic product, current prices (U.S. dollars)* and stores this series in a pandas dataframe. You can then view the dataframe with operations *data_frame.head()* or *print(date_frame)*

If some dimension is not specified, the method will consider all the elements in this dimension. Example::

    data_frame = knoema.get('IMFWEO2017Oct', subject = 'flibor6')

For multiple selection you can use the next examples::
  
    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country='Albania;Afghanistan;United States', subject='lp;ngdp')

or::

    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country=['Albania','Afghanistan','United States'], subject=['lp','ngdp'])


For case when the dimensions of dataset that have multi word names use the next example::

    import knoema
    data_frame = knoema.get('FDI_FLOW_CTRY', **{'Reporting country': 'AUS',
                                                    'Partner country/territory': 'w0',
                                                    'Measurement principle': 'DI',
                                                    'Type of FDI': 'T_FA_F',
                                                    'Type of entity': 'ALL',
                                                    'Accounting entry': 'NET',
                                                    'Level of counterpart': 'IMC',
                                                    'Currency': 'USD'})

If specified elements' names contain semicolons you can override default elements separator like that::

    import knoema
    data_frame = knoema.get('IMFWEO2019Oct',
        country='Albania',
        subject='Gross domestic product, constant prices (Percent change)|Gross domestic product per capita, constant prices (Purchasing power parity; 2011 international dollar)',
        separator='|')

In addition to the required using of the selections for dimensions, you can additionally specify the period and frequencies in the parameters. For more details, see the example below::

    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country='914;512;111', subject='lp;ngdp', frequency='A', timerange='2007-2017')

Also you can group results by one of dimensions::

    import knoema
    data_frame_generator = knoema.get('IMFWEO2017Oct', True, group_by='country', country='Albania;Afghanistan;United States', subject='lp;ngdp')
    for frame in data_frame_generator:
        data = frame.data
        metadata = frame.metadata

There is an advanced time mode where you can use multiple frequencies and different time selections::

    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country='914;512;111', subject='lp;ngdp', frequency='A', timelast='5')

    data_frame = knoema.get('IMFDOT2017', **{'Country': 'Algeria', 'Indicator': 'TXG_FOB_USD', 'Counterpart Country': '622', 'frequency': 'A;Q', 'timesince': '2010'})

    data_frame = knoema.get('IMFWEO2021Apr', Country='614', Subject='BCA', timemembers='1980;2002;2023')

The advanced time mode doesn't work with grouped results and columns.



******************************************************
Retrieving series from datasets including metadata
******************************************************
By default the function knoema.get returns the one dataframe with data. If you want also get information about metadata(attributes, unit, scale, mnemonics), include the additional parameter in your function, like this::

     import knoema
     data, metadata = knoema.get('IMFWEO2017Oct', True, country=['Albania', 'Afghanistan'], subject='lp')
     
The function, in this case, returns two dataframes - one with data, second with metadata.    

******************************************************
Accessing dimension hierarchy
******************************************************
Don't forget to import knoema library::

     import knoema

When you get data with knoema.get, dimension hierarchy is not automatically included. In this example, the locations ‘World’, ‘Africa’, and ‘Algeria’ are all returned as Location::

     df = knoema.get('kaziajg', frequency='D', Location='World;Africa;Algeria', Indicator='A1')

Let’s say that you want to look only at African countries. First, you need to get the dimension information for that dataset ID (“kaziajg”) and dimension (“Location”)::

     dims = knoema.dimension("kaziajg", "Location")

Then, you need to filter your data down to the location of interest::

     def filter_by_dimension_parent(_df, _dims, _parent):
         # Get data for only a subset within the hierarchy

         # Input the outputs of knoema.get; knoema.dimension; and the dimension member to filter on
         # Output the knoema.get results filtered down to only the dimension member specified by parent
         df_output = _df.copy()
         level = _df.columns.names.index(_dims.name)

         for column in _df:
             dim = column[level]
             df_dim = _dims.members[_dims.members['name'] == dim]
             if df_dim['parent name'].values[level] != _parent:
                 df_output = df_output.drop(column, axis=1)

         return df_output
         

     df_only_African_countries = filter_by_dimension_parent(df, dims, "Africa")

********************
Data Transformation
********************
You can use transform parameter to apply transformation to requested data, like this::

   import knoema
   data_frame = knoema.get('IMFWEO2017Oct', country='Afghanistan', subject='ngdp', transform='PCH')

The supported values of transform parameter are the following:

* PCH – % Change, a change from the previous month
* PCHY – % Change from a year ago, a change from the same month of the previous year 
* PCHA – % Change, annualized, a change from the previous month raised by 12 in the case of monthly data, and by 4 in the case of quarterly data.
* DIFF – Change, an absolute change from the previous month which represents value in the current month minus the value in the previous month.
* DIFFY – Change from a year ago
* DIFFA – Change, YTD
* DIFFYTD – Change, YTD (year to date), an absolute change from the beginning of the year
* DLOG – Log difference, the difference of natural logarithms of the current and previous period which is equivalent to the % change.
* DLOGY – Log Difference from a year ago
* DLOGYTD – Log Difference, YTD
* YTD – Year to date, the sum of values since the start of the year.
* ABS - the function that returns the absolute value of a number.

In order to get requested data normalized to specific frequency, you can specify frequency parameter, like this::

    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country='914;512;111', subject='lp;ngdp', frequency='M')

When the frequency of time-series is different from the value of Frequency parameter aggregation/disaggregation of data is performed.

For datasets with several date columns you can specify particular column with datecolumn parameter, like this::

    import knoema
    data_frame = knoema.get('bjxchy', country='Albania', measure='Original Principal Amount ($)', datecolumn='Effective Date (Most Recent)', timerange='2010-2015', frequency='A')
    
******************
Uploading Dataset
******************
In order to update the dataset, you must have the access rights to do this. For this, you need to specify the appropriate parameters app_id and app_secret. See section *Configuration*.

if you have access rights and file or pandas dataframe for uploading, use the next code::

    knoema.upload(file_path_or_frame, dataset=None, public=False, name=None)

where:

* file_path_or_frame - the string variable which provides path to the file which will be uploaded to the dataset or pandas dataframe,
* dataset - the string variable which provides id of the dataset that is going to be updated from the file. If dataset is None then new dataset will be created  based on the file,
* public - the boolean variable which makes dataset public if public flag is true. Default value is false,
* name - the string variable which provides name of the dataset

The function returns dataset id if upload is succesfull and raise an exception otherwise.


******************
Verifying Dataset
******************
In order to verify the dataset, you must have the access rights to do this. Please check if you are allowed to verify dataset with your Portal administrator and specify the appropriate parameters app_id and app_secret. See section *Configuration*.

if you have access rights, use the next code::

    knoema.verify('dataset_id', 'publication_date', 'source', 'refernce_url')

where:

* 'dataset_id' - the string variable which should provide id of the dataset that is going to be verified
* 'publication_date' - the datetime variable which should provide the date when dataset has been published
* 'source' - the string variable which should provide the source for the dataset (e.g. IMF)
* 'refernce_url' - the string variable which should provide URL to the source or a site from where the dataset has been downloaded


******************
Deleting Dataset
******************
In order to delete the dataset, you must have the access rights to do this. For this, you need to specify the appropriate parameters app_id and app_secret. See section *Configuration*.

if you have access rights, use the next code::

    knoema.delete('dataset_id')

where:

* 'dataset_id' - the string variable which should provide id of the dataset that is going to be deleted

**********************
Searching by mnemonics
**********************
The search by mnemonics is implemented in knoema. Mnemonics is a unique identifier of the series. Different datasets can have the same series with the same mnemonics. In this case, in the search results there will be a series that was updated last. The same series can have several mnemonics at once, and you can search for any of them. 
An example of using the search for mnemonics::

    data_frame = knoema.get('dataset_id', mnemonics = 'mnemonic1;mnemonic2')
    data_frame, metadata = knoema.get('dataset_id',True, mnemonics = ['mnemonic1','mnemonic2'])

If you are downloading data by mnemonics without providing dataset id, you can use this example::

    data_frame = knoema.get(mnemonics = 'mnemonic1;mnemonic2')
    data_frame = knoema.get(None, mnemonics = 'mnemonic1;mnemonic2')
    data_frame, metadata = knoema.get(dataset = None, include_metadata = True, mnemonics = ['mnemonic1','mnemonic2'])

******************
Searching by query
******************
You can also make a search for arbitrary query using knoema search engine::

    res = knoema.search('Italy GDP')
    for series in res.series:
        print('{} ({})'.format(series.title, series.dataset))

Also every series in res has get() method to load data for it::

    series_data = res[0].get()

*******************************************************
Possible errors in Knoema package and how to avoid them
*******************************************************
1. "ValueError: Dataset id is not specified"

This error appears when you use None instead dataset's Id.
Example::

    knoema.get(None)

2. "ValueError: Dimension with id or name some_name_of_dimension is not found"

This error appears when you use name that doesn't correspond to any existing dimensions' names or ids.
Examples::

    knoema.get('IMFWEO2017Oct', dimension_not_exist='914', subject='lp')
    knoema.get('IMFWEO2017Oct', **{'dimension not exist':'914', 'subject':'lp'})

3. "ValueError: Selection for dimension dimension_name is empty"

This error appears when you use empty selection for dimension .
Examples::

    knoema.get('IMFWEO2017Oct', country ='', subject='lp')
    knoema.get('IMFWEO2017Oct', **{'country':'914', 'subject':''})

4. "ValueError: Requested dataset doesn't exist or you don't have access to it"

This error appears when you use dataset that doesn't exist or you don't have access rights to it.
Example::

    knoema.get('IMFWEO2017Apr1', **{'country':'914', 'subject':'lp;ngdp'})

This dataset doesn't exist. If your dataset exist, and you have access to it, check that you set api_config with app_id and app_secret.

5. "ValueError: "Underlying data is very large. Can't create visualization"

This error appears when you use a big selection. Try to decrease the selection.

6. "The specified host incorect_host doesn't exist"

This error appears when you use host that doesn't exist.
Example::

    apicfg = knoema.ApiConfig()
    apicfg.host = 'knoema_incorect.com'
    data_frame = knoema.get('IMFWEO2017Oct', country='914', subject='ngdp')

7. "HTTPError:  HTTP Error 400: Bad Request"

This error appears when you try to delete dataset that doesn't exist or you don't have access rights to it.
Example::

    knoema.delete('nonexistent_dataset')

If you have access to it, check that you set api_config with app_id and app_secret.

8. "HTTPError: HTTP Error 403: The number of requests for /api/meta/dataset/datasetId/dimension/dimensionId exceeds 50"

This error appears when you use public user (api_config without app_id and app_secret parameters set) and reached the limit of requests.
You can avoid this error, using api_config with app_id and app_secret.

9. "HTTPError: HTTP Error 403: The number of requests for /api/meta/dataset/datasetId/dimension/dimensionId exceeds 500"

This error appears when you use api_config with app_id and app_secret parameters set, and reached the limit of requests.
You can avoid this error, using other parameters app_id and app_secret.

10. "HTTPError: HTTP Error 403: invalid REST authentication credentials"

This error appears when you try to use api_config with app_id and app_secret, but they are incorrect. 
You can avoid this error, using other parameters app_id and app_secret.

11. "AttributeError: 'str' object has no attribute 'strftime'"

This error appears when you use string data instead datetime.
Example::

    knoema.verify('IMFWEO2017Oct','2017-5-7','IMF','http://knoema.com')

You can avoid this error using datetime instead string date.
Example::

    knoema.verify('IMFWEO2017Oct',datetime(2017,5,7),'IMF','http://knoema.com')

12. "ValueError: The function does not support the simultaneous use of mnemonic and selection"
This error appears when you use mnemonics and selection in one query.
Example::

    knoema.get('IMFWEO2017Oct', mnemonics = 'some_mnemonic', country ='912', subject='lp')
    knoema.get(None, mnemonics = 'some_mnemonic', country = 'USA')

13. "ValueError: Selection for dimension dimension_name contains invalid elements"

This error appears when any of the specified elements don't exist.
Examples::

    knoema.get('IMFWEO2017Oct', **{'country':'914', 'subject':'nonexistent_element1; nonexistent_element2'})

14. "ValueError: Only one parameter should be passed: timerange, timesince, timelast, timemebers"
This error appears when you use several time modes at a time.
Example::

    knoema.get('IMFWEO2017Oct', country='914', subject='ngdp', timesince='2000', timerange='2000-2010')

15. "ValueError: Advanced time modes and multiple frequencies can't be used with group_by or columns parameters"
This error appears when you use advanced time mode and/or mupltiple frequencies with group_by or columns parameters passed.
Example::

    knoema.get('IMFWEO2017Oct', country='914;900', subject='ngdp', timesince='2000', group_by='country')