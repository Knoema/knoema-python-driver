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
   data_frame = knoema.get('IMFWEO2017Apr', country='914', subject='ngdp')

where:

* 'IMFWEO2017Apr' this is a public dataset, that available for all users by reference https://knoema.com/IMFWEO2017Apr.
* country and subject are dimensions names
* '914' is id of country *Albania*
* 'ngdp' is id of subject *Gross domestic product, current prices (U.S. dollars)*

This example finds all data points for the dataset IMFWEO2017Apr with selection by country = *Albania* and subject =  *Gross domestic product, current prices (U.S. dollars)* and stores this series in a pandas dataframe. You can then view the dataframe with operations *data_frame.head()* or *print(date_frame)*

Please note that you need to identify all dimensions of the dataset, and for each dimension to indicate the selection. Otherwise, the method returns an error.

For multiple selection you can use the next example::
  
    import knoema
    data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp')

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

In addition to the required using of the selections for dimensions, you can additionally specify the period and frequencies in the parameters. For more details, see the example below::

    import knoema
    data_frame = knoema.get('IMFWEO2017Apr', country='914;512;111', subject='lp;ngdp', frequency='A', timerange='2007-2017')

******************
Uploading Dataset
******************
In order to update the dataset, you must have the access rights to do this. For this, you need to specify the appropriate parameters app_id and app_secret. See section *Configuration*.

if you have access rights and file for uploading, use the next code::

    knoema.upload('C:\\Path\\File.csv', 'dataset_id')

or::

    knoema.upload('C:\\Path\\File.zip', 'dataset_id')


******************
Verifying Dataset
******************
In order to verify the dataset, you must have the access rights to do this. For this, you need to specify the appropriate parameters app_id and app_secret. See section *Configuration*.

if you have access rights and file for uploading, use the next code::

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

if you have access rights and file for uploading, use the next code::

    knoema.delete('dataset_id')

where:

* 'dataset_id' - the string variable which should provide id of the dataset that is going to be deleted