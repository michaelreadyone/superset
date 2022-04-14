import pandas as pd
from typing import Dict, List, Any

def map_to_pandas_type(col_type: str) -> str:
    map_dict = {
        'INTEGER': 'int32',
        'TEXT': 'string'
    }
    return map_dict[col_type]

def pandas_to_superset_type(col_type: str) -> str:
    map_dict = {
        'int32': 'INT',
        'string': 'TEXT'
    }
    return map_dict[col_type]

def parse_flat_file(filepath: str, col_infos: Dict[str, Any], json_sql: Dict[str, Any]) -> List[Dict[str, Any]]:
    flat_cols = [col['column_name'] for col in col_infos]
    types = [col['type'] for col in col_infos]
    types = [map_to_pandas_type(col_type) for col_type in types]

    df = pd.read_csv(filepath)
    df_cols = list(df.columns.values)

    output_cols = []
    columns = []
    for col, col_type in zip(flat_cols, types):
        # print('col: ', col)
        # print('df_cols: ', df_cols)
        if col not in df_cols:
            raise RuntimeError(f'column info "{col}" not found from flatfile')
        df[col] = df[col].astype(col_type)
        columns.append({
            'name':col,
            'type':pandas_to_superset_type(col_type),
            'is_date':False
        })
        output_cols.append(col)
    if type(json_sql['SELECT']) != list:
        raise RuntimeError('SELECT must be followed by a list')
    if json_sql['SELECT'] == ['*']:
        json_sql['SELECT'] = output_cols
    selected_cols = [col for col in json_sql['SELECT'] if col in output_cols]
    df_out = df[selected_cols]

    try:
        col, compare, val = json_sql['WHERE'].split()
        df_out = df_out[(eval('df_out["' + col + '"]' + compare + val))]
    except:
        pass

    selected_columns = [col for col in columns if col['name'] in selected_cols]

    data = []
    col_names = list(df_out.columns)
    for row in df_out.iterrows():
        dic = {}
        for i in range(len(col_names)):
            dic[col_names[i]] = list(row[1])[i]
        data.append(dic)
    if len(selected_cols) == 0:
        raise RuntimeError('No results, please check your query')
    res = {
        'data':data,
        'columns':selected_columns,
        'selected_columns':selected_columns,
        'query':{
            'limit':100,
        }
    }

    return res


