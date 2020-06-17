import tempfile, os, shutil, string, random
import numpy as np
import pandas as pd
from pathlib import Path
from abc import ABC, abstractmethod
import csv

class FrameTransformerFactory(object):

    def __init__(self, frame):
        self._frame = frame

    def _get_column_types(self):
        date_columns = []
        value_columns = []
        dim_columns = []

        for col in self._frame.columns:
            is_numeric = True
            is_date = True

            try:
                pd.to_numeric(self._frame[col])
                value_columns.append(col)
            except ValueError:
                is_numeric = False

            if not is_numeric:
                try:
                    pd.to_datetime(self._frame[col])
                    date_columns.append(col)
                except ValueError:
                    is_date = False

            if not is_numeric and not is_date:
                dim_columns.append(col)

        return dim_columns, date_columns, value_columns

    def get_transformer(self):
        dim_columns, date_columns, value_columns = self._get_column_types()

        if len(date_columns) > 1 or (len(dim_columns) > 0 and len(value_columns) > 1):
            return FrameTransformerFlat(self._frame, dim_columns, date_columns, value_columns)
        
        return FrameTransformerRegular(self._frame, dim_columns, date_columns, value_columns)

class FrameTransformerBase(ABC):

    def __init__(self, frame, dim_columns, date_columns, value_columns):
        self._frame = frame
        self._dim_columns = dim_columns
        self._date_columns = date_columns
        self._value_columns = value_columns

        if frame is None:
            raise ValueError('Frame is not specified.')

        if frame.empty:
            raise ValueError('Frame cannot be empty.')

    @abstractmethod
    def prepare(self, dataset = None, dataset_name = None):
        return

class FrameTransformerFlat(FrameTransformerBase):

    def __init__(self, frame, dim_columns, date_columns, value_columns):
        super().__init__(frame, dim_columns, date_columns, value_columns)

    def _rows(self, frame):
        data_rows = []
        first_data_row = []

        for col in frame.columns:
            first_data_row.append(col)

        data_rows.append(first_data_row)
        nan_frame = frame.isnull()

        for i in range(len(frame.values)):
            frame_row = frame.values[i]
            nan_frame_row = nan_frame.values[i]

            row = []

            for j in range(len(frame_row)):
                item = frame_row[j]
                nan_item = nan_frame_row[j]
                row.append('' if nan_item else str(item))

            data_rows.append(row)

        return data_rows

    def prepare(self, file_wrapper, dataset = None, dataset_name = None):
        rows = self._rows(self._frame)

        file_name = (dataset_name if dataset_name != None else dataset) + '.csv'
        return file_wrapper.write_single_file(file_name, rows)

class FrameTransformerRegular(FrameTransformerBase):

    def __init__(self, frame, dim_columns, date_columns, value_columns):
        super().__init__(frame, dim_columns, date_columns, value_columns)

    def _get_axis_types(self):
        return [type(axes).__name__ for axes in self._frame.axes]

    def _change_columns_types(self, date_columns, value_columns):
        for date_column in date_columns:
            self._frame[date_column] = pd.to_datetime(self._frame[date_column])

        for value_column in value_columns:
            self._frame[value_column] = pd.to_numeric(self._frame[value_column])

    def _prepare_frame(self):
        ready_to_upload_frame = None

        axes_types = self._get_axis_types()

        self._change_columns_types(self._date_columns, self._value_columns)

        if 'DatetimeIndex' not in axes_types:
            if len(self._date_columns) == 0:
                raise ValueError('The frame has no column with dates.')

            if len(self._date_columns) == 1:
                self._frame.set_index(self._date_columns[0], drop = True, append = False, inplace = True)

            if len(self._value_columns) > 1:
                raise ValueError('The frame has few columns with dates.')

        if 'MultiIndex' in axes_types:
            ready_to_upload_frame = self._frame
        else:
            if len(self._dim_columns) == 0:
                if len(self._value_columns) == 0:
                    raise ValueError('The frame doesn\'t have column with values.')

                ready_to_upload_frame = self._frame
            else:
                if len(self._value_columns) == 0:
                    raise ValueError('The frame doesn\'t have column with values.')

                if len(self._value_columns) == 1:
                    ready_to_upload_frame = self._frame.pivot_table(
                        index = self._date_columns[0],
                        columns = self._dim_columns if len(self._dim_columns) > 0 else None,
                        values = self._value_columns[0]
                    )
                else:
                    raise ValueError('The frame has more than one column with values.')
                
        return ready_to_upload_frame

    def _parse_dimension_list(self, full_dim_list):
        freq_index = -1
        scale_index = -1
        unit_index = -1

        dimensions = []

        if len(full_dim_list) == 1 and full_dim_list[0] == None:
            full_dim_list = ['Dimension1']

        for i in range(len(full_dim_list)):
            dim = full_dim_list[i]
            if dim == 'Frequency':
                freq_index = i
                continue

            if dim == 'Scale':
                scale_index = i
                continue

            if dim == 'Unit':
                unit_index = i
                continue

            dimensions.append(full_dim_list[i])

        return dimensions, freq_index, scale_index, unit_index

    def _dataset_sheet(self, dataset_id, dataset_name, dimensions):
        rows = []
        rows.append(['Name', 'Value'])
        if dataset_id != None:
            rows.append(['Dataset', dataset_id])
        if dataset_id != None or dataset_name != None:
            rows.append(['Dataset name', dataset_name if dataset_name != None else dataset_id])
        rows.append(['Dimensions', ';'.join(dimensions)])
        rows.append(['Data', 'Data'])
        return rows

    def _get_unic_id(self, name, map):
        if name in map:
            return map[name]

        parts_raw = name.split(' ')
        parts = []
        for part in parts_raw:
            new_part = ''.join(p for p in part if p.isalnum())
            if new_part == '':
                continue
                    
            parts.append(new_part.upper())

        if len(parts) == 1:
            return parts[0].upper()[:8]

        if len(parts) <= 3:
            res = ''
            for part in parts:
                res += part.upper()[:3]
            return res

        res = ''
        for part in parts:
            res += part.upper()[0]

        counter = 1
        res_with_counter = res
        while True:
            if res_with_counter in map.values():
                res_with_counter = res + '_' + str(counter)
                counter += 1
            else:
                res = res_with_counter
                break
                
        return res

    def _dimension_sheets(self, dimensions, series_names):
        dimensions_rows = {}
        dimensions_map = {}
        
        for dim_ind in range(len(dimensions)):
            dim = dimensions[dim_ind]
            dim_rows = []
            dim_map = {}
            first_row = ['Name', 'Code']

            dim_rows.append(first_row)

            for name in series_names:
                part = name if len(dimensions) == 1 else name[dim_ind]
                id = self._get_unic_id(part, dim_map)
                row = [part, id]

                if part in dim_map:
                    continue

                dim_map[part] = id

                dim_rows.append(row)

            dimensions_rows[dim] = dim_rows
            dimensions_map[dim] = dim_map

        return dimensions_rows, dimensions_map

    def _freq_fetch(self, pandas_freq):
        parts = pandas_freq.split('-')
        freq = parts[0]

        # https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
        if freq in ['B', 'C', 'D']: return 'D'
        if freq in ['W']: return 'W'
        if freq in ['M', 'SM', 'BM', 'CBM', 'MS', 'BMS', 'CBMS']: return 'M'
        if freq in ['Q', 'BQ', 'QS', 'BQS']: return 'Q'
        if freq in ['A', 'Y', 'BA', 'BY', 'AS', 'YS', 'BAS', 'BYS']: return 'A'

        return None

    def _data_sheet(self, frame, dimensions, dimensions_map, freq_index, scale_index, unit_index, series_names):
        data_rows = []
        first_data_row = []
        for d_n in dimensions:
            first_data_row.append(d_n)

        if scale_index >= 0:
            first_data_row.append('Scale')

        if unit_index >= 0:
            first_data_row.append('Unit')

        first_data_row.append('Frequency')

        first_data_row.extend(['Date', 'Value'])
        data_rows.append(first_data_row)

        for name in series_names:
            series = frame[name]
            nan_series = series.isnull()
            for ind in range(len(frame.index)):
                row = []

                if len(dimensions) == 1:
                    map = dimensions_map[dimensions[0]]
                    row.append(map[name])

                    freq = None
                    if frame.axes[0].freq == None:
                        freq = self._freq_fetch(pd.infer_freq(frame.axes[0]))
                    else:
                        freq = self._freq_fetch(self._frame.axes[0].freq.name)

                    if freq == None:
                        raise ValueError('Wrong frequency.')

                    row.append(freq)
                else:
                    for part_ind in range(len(name)):
                        if scale_index >= 0 and part_ind == scale_index:
                            continue

                        if unit_index >= 0 and part_ind == unit_index:
                            continue

                        if freq_index >= 0 and part_ind == freq_index:
                            continue
                            
                        part = name[part_ind]

                        dim_name = frame.columns.names[part_ind]
                        if dim_name in dimensions_map:
                            map = dimensions_map[dim_name]
                            part = map[part]

                        row.append(part)

                    if scale_index >= 0:
                        row.append(name[scale_index])

                    if unit_index >= 0:
                        row.append(name[unit_index])

                    if freq_index >= 0:
                        row.append(name[freq_index])

                    if freq_index == -1:
                        freq = None
                        if frame.axes[0].freq == None:
                            freq = self._freq_fetch(pd.infer_freq(frame.axes[0]))
                        else:
                            freq = self._freq_fetch(self._frame.axes[0].freq.name)

                        if freq == None:
                            raise ValueError('Wrong frequency.')

                        row.append(freq)

                row.append(frame.index[ind])
                row.append(series[ind] if not nan_series[ind] else '')

                data_rows.append(row)

        return data_rows

    def prepare(self, file_wrapper, dataset = None, dataset_name = None):
        frame = self._prepare_frame()

        dimensions, freq_index, scale_index, unit_index = self._parse_dimension_list(frame.columns.names)

        dataset_rows = self._dataset_sheet(dataset, dataset_name, dimensions)

        series_names = frame.columns.values
        dimensions_rows, dimensions_map = self._dimension_sheets(dimensions, series_names)

        data_rows = self._data_sheet(frame, dimensions, dimensions_map, freq_index, scale_index, unit_index, series_names)

        file_wrapper.add_to_archive('Dataset.csv', dataset_rows)
        for dim in dimensions_rows:
            file_wrapper.add_to_archive('{}.csv'.format(dim), dimensions_rows[dim])
        file_wrapper.add_to_archive('Data.csv', data_rows)

        return file_wrapper.get_archive()

class FileLayerWrapper(object):

    def __init__(self):
        self._tmp_dir = None
        self._tmp_file = None

    def write_single_file(self, name, rows):
        parent_folder = tempfile.gettempdir()
        self._write_file(parent_folder, name, rows)

        self._tmp_file = os.path.join(parent_folder, name)
        return self._tmp_file

    def add_to_archive(self, name, rows):
        if self._tmp_dir == None:
            self._create_tmp_dir()

        self._write_file(self._tmp_dir, name, rows)

    def get_archive(self):

        shutil.make_archive(self._tmp_dir, 'zip', self._tmp_dir)
        self._tmp_file = self._tmp_dir + '.zip'
        return self._tmp_file

    def _create_tmp_dir(self):
        parent_folder = tempfile.gettempdir()
        tmp_dir = os.path.join(parent_folder, self._get_tmp_dir_name())

        os.makedirs(tmp_dir, exist_ok=True)

        self._tmp_dir = tmp_dir

    def _get_tmp_dir_name(self, len = 8):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(len))

    def _write_file(self, path, name, rows):
        with open(os.path.join(path, name), 'w', newline = '') as file:
            writer = csv.writer(file)
            writer.writerows(rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._tmp_dir != None and os.path.exists(self._tmp_dir):
            shutil.rmtree(self._tmp_dir)

        if self._tmp_file != None and os.path.isfile(self._tmp_file):
            os.remove(self._tmp_file)

