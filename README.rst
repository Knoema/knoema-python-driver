========================================
Official Python package for Knoema's API
========================================

This is the official documentation for Knoema's Python Package. The package can be used for obtaining data from the datasets from the site knoema.com and uploading data to datasets. This package is compatible with Python v3.x+.

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
By default the package allows you to work only with public datasets from the site knoema.com.

If you want work with private datasets, you need to use the next code::

    import knoema
    apicfg = knoema.ApiConfig()
    apicfg.host = 'knoema.com'
    apicfg.app_id = "some_app_id"
    apicfg.app_secret = "some_app_secret"

You can get parameters app_id and app_secret after registering on the site knoema.com, in the section "My profile - Apps - create new" (or use existing applications).

Also, you can use other hosts supported by knoema.

*******************************
Retrieving series from datasets
*******************************
There are one method for retrieving series from datasets in Python: the **get** method. The method works with knoema datasets.

The following quick call can be used to retrieve a timeserie from dataset::

   import knoema
   data_frame = knoema.get('IMFWEO2017Oct', country='914', subject='ngdp')

where:

* 'IMFWEO2017Oct' this is a public dataset, that available for all users by reference https://knoema.com/IMFWEO2017Oct.
* country and subject are dimensions names
* '914' is id of country *Albania*
* 'ngdp' is id of subject *Gross domestic product, current prices (U.S. dollars)*

This example finds all data points for the dataset IMFWEO2017Oct with selection by country = *Albania* and subject =  *Gross domestic product, current prices (U.S. dollars)* and stores this series in a pandas dataframe. You can then view the dataframe with operations *data_frame.head()* or *print(date_frame)*

If some dimension is not specified, the method will consider all the elements in this dimension. Example::

    data_frame = knoema.get('IMFWEO2017Oct', subject = 'flibor6')

For multiple selection you can use the next examples::
  
    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country='914;512;111', subject='lp;ngdp')

or::

    import knoema
    data_frame = knoema.get('IMFWEO2017Oct', country=['914','512','111'], subject=['lp','ngdp'])


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

******************************************************
Retrieving series from datasets including metadata
******************************************************
By default the function knoema.get returns the one dataframe with data. If you want also get information about metadata(attributes, unit, scale, mnemonics), include the additional parameter in your function, like this::

     import knoema
     data, metadata = knoema.get('IMFWEO2017Oct', True, country=['914','512'], subject='lp')
     
The function, in this case, returns two dataframes - one with data, second with metadata.    

********************
Data Transformation
********************
You can use transform parameter to apply transformation to requested data, like this::


   import knoema
   data_frame = knoema.get('IMFWEO2017Oct', country='914', subject='ngdp', transform='PCH')

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

if you have access rights and file for uploading, use the next code::

    knoema.upload(file_path, dataset=None, public=False)

where:

* file_path - the string variable which provides path to the file which will be uploaded to the dataset,
* dataset - the string variable which provides id of the dataset that is going to be updated from the file. If dataset is None then new dataset will be created  based on the file,
* public - the boolean variable which makes dataset public if public flag is true. Default value is false.

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

