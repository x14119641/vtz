import requests
import time
import pandas as pd
import re

import lxml.html as lh


class BLSscraper():
    main_url = 'https://download.bls.gov/pub/time.series/'

    def __init__(self):
        self.key_names = self.read_key_names()

    def read_key_names(self):

        my_list = requests.get(
            BLSscraper.main_url+'overview.txt'
        ).text.split('\r\n')
        for i, item in enumerate(my_list):
            if 'LIST OF DATABASES' in item:
                start = i
            elif 'DIRECTORY STRUCTURE' in item:
                end = i
        my_list = my_list[start+1:end]
        d = {}
        for item in my_list:
            item = item.split()
            if item:
                d[item[0].lower()] = ' '.join(item[1:])

        return d

    def read_main_series(self, key):
        data = requests.get(
            f'{BLSscraper.main_url}{key}/{key}.series')
        return data.text

    def convert_str_to_df(self, data_str):

        for i, line in enumerate(data_str.splitlines()[:5]):
            print(line)
            line = line.split('\t')
            print(line)
            print(len(line))
            if not i:
                if 'end_period' in line[-1]:

                    df = pd.DataFrame(columns=[col for col in line if col])
                else:
                    print('END PERIOD')
                    # First row altogether with column values
                    len_to_split = len(data_str.splitlines()[1].split('\t'))

                    column = line[:len_to_split]

                    first_item_row = column[-1].split('end_period')[-1].strip()
                    column[-1] = 'end_period'
                    column[0] = column[0].strip()
                    line = list(line[len_to_split:])
                    line.insert(0, first_item_row)
                    # Create d with columns and insert first row
                    df = pd.DataFrame(columns=column)
                    s = pd.Series(line, index=df.columns)
                    df = df.append(s, ignore_index=True)
            else:
                # print('Line --> ', re.split(r'\s{2,}', line))
                line[0] = line[0].strip()
                if len(df.columns) < len(line):
                    if not line[-1]:
                        print('jERE')
                        # hs
                        line = [item for item in line if item.strip()]

                    elif 'footnote_codes' not in df.columns:
                        # ee,eb
                        print('KERE')
                        new = list(df.columns)
                        new.insert(-4, 'footnote_codes')

                        df = pd.DataFrame(columns=new)
                    else:
                        # dont need at the moment
                        line = [item for item in line if item.strip()]

                s = pd.Series(line, index=df.columns)
                df = df.append(s, ignore_index=True)
            return df

    def clean_columns(self, columns):
        replace_dict = {
            "data_type_code": "datatype_code",
            "industryb_code": "industrybase_code",
            "mog_code": "mogocc_code",
            "seasonality": "seasonal",
            "job": "job_characteristic_code"
        }
        return [replace_dict.get(item, item) for item in columns]

    def get_codes_names(self, columns):
        codes_names_to_append = ['seasonal']

        codes_names = [name_code.split('_')[0] for name_code in columns if 'code' in name_code]
        return codes_names + codes_names_to_append

    def get_codes_data(self, key):
        data = requests.get(
            f'{BLSscraper.main_url}{key}/')
        data = lh.fromstring(data.text)
        x = [item.split('.')[1] for item in data.xpath('//a/text()')[1:]]
        return x

    def iterate_codes_list_and_build_df(self, data):
        res = []
        print(data)
        for item in data:
            print(item)
            # item = item.split('\r')
            # item = [re.split(r'\s{2,}', ite) for ite in item]

            if not item[0]:

                res[-1][-1] = f'{res[-1][-1]} {item[-1]}'

            else:
                res.append(item)

        return res

    def checking_codes_names(self, codes_names, codes_data):
        print(set(codes_names).issubset(codes_data))

    def format_columns(self, columns):
        pass

    def format_data_str(self, data_str):
        '''format ---- data_str'''

        data_str = re.findall(r'-{1,}[\s\S]*$', data_str)[0]
        data_str = re.findall(r'[A-z0-9][\s\S]*$', data_str)[0]

        data_str = data_str.split('\r\n')
        data_str = [re.split(r'\s{2,}', ite) for ite in data_str]

        return data_str

    def format_codes_special_chrs(self, data_str):

        if not '- -' in data_str:

            data_str = data_str.replace(key.upper() + ' Measure', '')
            data_str = re.findall(r'[a-zA-z0-9][\s\S]*$', data_str)[0]

            data_str = data_str.split('\r\n')
            data_str = [re.split(r'\s{2,}', ite) for ite in data_str]

        else:
            data_str = bls.format_data_str(data_str)

        res = bls.iterate_codes_list_and_build_df(data_str)
        df = pd.DataFrame(res)
        return df

    def read_tables(self, key, table_key):
        if '_' in table_key:
            table_key = table_key.replace('_', '.')
        elif 'srd' == table_key and key != 'ml':
            table_key = 'state_region_division'
        elif 'periodicity' == table_key and key == 'li':
            table_key = 'period'
        elif 'job' == table_key:
            print('IN THE JOB')
            table_key = 'job_characteristic'
        if table_key == 'soc':
            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.occupation')
        elif 'hs' == key:
            print('datatypeeeeeeeee hsss')
            if 'case' == table_key:
                table_key = 'case.type'
            elif 'datatype' == table_key:
                table_key = 'data.type'
            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        elif 'ii' == key:
            print('datatypeeeeeeeee ii')
            if 'case' == table_key:
                table_key = 'case_type'

            elif 'datatype' == table_key:
                table_key = 'data_type'
            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        elif 'nw' == key:
            print('datatypeeeeeeeee nw')
            if table_key in ['state', 'area']:
                data = requests.get(
                    f'{BLSscraper.main_url}{key}/{key}.starea')
            elif table_key in ['estimate', 'subcell', 'datatype']:
                data = requests.get(
                    f'{BLSscraper.main_url}{key}/{key}.{table_key}_id')
            else:
                data = requests.get(
                    f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        elif 'oe' == key:
            print('INT HTE STATE')
            if table_key == 'state':
                data = requests.get(
                    f'{BLSscraper.main_url}{key}/{key}.area')

            else:
                data = requests.get(
                    f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        # elif 'sa' == key:
        #     if table_key == 'datatype':
        #         table_key = 'data_type'
        #     data = requests.get(
        #         f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        elif 'sh' == key:
            if table_key == 'datatype':
                table_key = 'data.type'
            elif table_key == 'case':
                table_key = 'case.type'
            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        elif key in ['si', 'sa', 'sm']:
            if table_key == 'datatype':
                table_key = 'data_type'
            elif table_key == 'case':
                table_key = 'case_type'
            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.{table_key}')

        else:

            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.{table_key}')
        if not data:
            print('Reduing the data ')
            data = requests.get(
                f'{BLSscraper.main_url}{key}/{key}.{table_key}')

        print('data --> ', str(data))
        if '404' in str(data):
            print('PAGE NOT EXIST\n')
            return None
        print(str(data.text))

        print(data.url)
        # print('Line --> ', re.split(r'\s{2,}', data.text))
        # x = data.text.split('\t')
        if key.upper() in data.text[:4]:
            print('TRUE')
            res = bls.format_codes_special_chrs(data.text)
            print(res)
            return res
        elif key == 'nc' and table_key == 'area':
            print('IN THE nc.area  PUTO')
            print('##############\n'*5)
            x = data.text.split('\r\n')
            x = [item.split('\t')[1:3] for item in x if item.split('\t')]
        elif key == 'nc'and table_key == 'occupation':
            print('IN THE OCCUPATION MA NI')
            x = data.text.split('\r\n')
            x = [item.split('\t')[:4] for item in x if item.split('\t')]
            x = [[''.join(item[:2]), item[-1]] for item in x]
        elif key == 'pd' and table_key in ['product', 'industry']:
            print('IN THE OEEOEOEOEOEOE PUTO')
            print('##############\n'*5)
            x = data.text.split('\r\n')
            x = [item.split('\t') for item in x if item.split('\t')]
            x = [[it for it in item if it] if item else item for item in x]
        else:

            print('TABA')
            x = re.split(r'\s{2,}', data.text)
            print(x)

            x = [item.split('\t') for item in x if item.split('\t')]
        print(' in th exx::\n'*5)
        print(set([len(z) for z in x]))
        print([z for z in x if len(z) == 1])
        print(' in th exx::\n'*5)
        if len(x[-1]) == 1:
            print('EMpty')
            x = x[:-1]
        if key == 'hc' and (
                table_key == 'occupation' or table_key == 'nature'):
            print('HC EXCPETION')
            del x[1:3]
        if len(x) == 1:
            # When empty codes
            print('len(x)==1')
            pass

        elif len(x[0]) > len(x[1]) and len(x[1]) > len(x[2]):
            # cc
            print('len(x[0]) > len(x[1]) and len(x[1]) > len(x[2])')
            # print(x)
            # print(x[::2])
            # print(x[1::2])
            x[1:] = [x + y for x, y in zip(x[1::2], x[::2][1:])]
            # for i,item in enumerate(x[1:]):
            #     print(i, '   ',item)

            # x[1:] = [sum(x[1::i+2]) for i in range(0,len(x[:1]),2)]
        elif len(x[0]) < len(x[1]):
            # cd. missing "indixes"
            print('len(x[0]) < len(x[1])')
            x[1:] = [item[:-1] for item in x[1::2]]
            if len(x[-1]) > len(x[0]):
                x = x[:-1]
        elif len(x[0]) > len(x[1]):
            print('len(x[0]) > len(x[1])')
            if key == 'ml' and table_key == 'srd':
                print('IN THE SLR ML PUTO')
                x = [item[0:-1] for item in x[::2]]
                x.insert(0, [f'{table_key}_code', f'{table_key}_text'])
            # elif key == 'nc' and table_key == 'area':
            #     print('IN THE nc.area  PUTO')
            #     x = [item[1:-1] for item in x[::2]]
            #     x.insert(0, [f'{table_key}_code', f'{table_key}_text'])
            else:
                x[0] = x[0][1:]
        try:
            # print(x)
            if len(x) == 2 and len(x[0]) == 2:
                print('list of two')
                if key == 'ml':
                    df = pd.DataFrame(x,
                                      columns=[f'{table_key}_code',
                                               f'{table_key}_text'])

                else:
                    df = pd.DataFrame([x[1]],
                                      columns=x[0])

            elif len(x) == 2 and len(x[0]) == 1:
                print('HEREEEEEEEEEE')
                x = [x[0][0], x[1][0]]
                print(x)
                df = pd.DataFrame([x],
                                  columns=[f'{table_key}_code',
                                           f'{table_key}_text'])
            elif any('code' not in item for item in x[0]) and len(x[0]) == 2:
                df = pd.DataFrame(x,
                                  columns=[f'{table_key}_code',
                                           f'{table_key}_text'])
            else:
                df = pd.DataFrame(x[1:],
                                  columns=self.clean_columns(x[0]))
            return df
        except Exception as e:
            print(x)
            print(str(e))


if __name__ == '__main__':
    bls = BLSscraper()
    # bls.read_tables('ap', 'area')

    for i, key in enumerate(list(bls.key_names)[0:]):

        print('KEY ---> ', key, ', i:', i)
        start = time.perf_counter()
        data_str = bls.read_main_series(key)

        main_series = bls.convert_str_to_df(data_str)
        if isinstance(main_series, pd.DataFrame):
            print(main_series.head(10))

            name_codes = bls.get_codes_names(
                bls.clean_columns(main_series.columns))

        else:
            print('IS instance')
            continue
        # codes_data = bls.get_codes_data(key)
        # # bls.checking_codes_names(name_codes,codes_data)
        print(name_codes)
        for code in name_codes:
            if code == 'labor':
                code = 'labor_force'
            if key == 'nw' and code == 'seasonal':
                # not exists
                print('nw and noc')
                continue
            print(key, code)
            df = bls.read_tables(key, code)

            print('The df:\n', df)
            # print(df.columns)
            # print(df.iloc[:, :2])
            if not isinstance(df, pd.DataFrame):
                break
        print(time.perf_counter()-start)
        print('****\n')

        time.sleep(3)
        # break
