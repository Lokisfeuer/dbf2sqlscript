from dbfread import DBF
import pandas as pd
import string
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

FILENAME = 'filename'  # enter filename without extension


def main():
    data = DBF(FILENAME + '.dbf', encoding='ISO8859-1')
    df = to_df(data)
    # df = pd.read_csv('out.csv', delimiter=',')  # to save time during tests
    df.to_csv('out.csv')
    df = df.astype(str)
    script = sql_script(df)
    with open('out.sql', 'w') as f:
        f.write(script)


def to_df(data):
    df = pd.DataFrame(data=data.records)
    for column in df:
        val = df[column][0]
        if isinstance(val, str):
            if len(val) == 160:
                for i in val[:10]:
                    assert i in ['1', '0']  # if assert goes wrong it's not a code field
                df = replace_column(df, column_name=column)
    return df


def replace_column(df, column_name):
    # replace code column with 160 columns for each possible entry.
    with open(FILENAME + '.dbx', 'r') as f:
        data = f.read()
    data = data.split('\n')
    idx = data.index(f'   Name={column_name}')
    new_columns = data[idx + 4:idx + 4 + 160]
    new_columns.insert(0, f'\tText={column_name}_IGNORE_X')
    new_columns.append(f'\tText={column_name}_IGNORE_Y')
    new_columns = pd.DataFrame(new_columns)
    new_columns = new_columns[0].str[6:]
    df[new_columns] = df[column_name].str.split('', expand=True)  # performance warning
    df.drop([column_name, f'{column_name}_IGNORE_X', f'{column_name}_IGNORE_Y'], axis=1, inplace=True)
    return df


def sql_script(df):
    name = 'all_data'  # name of the table in the sql database

    # clean up column names.
    columns = df.columns.str.lower()  # make everything lowercase
    for i in string.punctuation:
        columns = columns.str.replace(i, 'x')  # replace special characters with "x"
    for i in string.whitespace:
        columns = columns.str.replace(i, '_')  # replace whitespaces with "_"
    columns = tuple(columns)

    # iterate over column to generate the CREATE TABLE command
    table = ''
    for column in columns:
        datatype = 'varchar'  # one could optimize this part but why...
        table += f'\n\t{column} {datatype},'
    table = table[:-1]  # deleting last comma
    create_table_prompt = f'CREATE TABLE {name} ({table}\n)\n\n'

    # iterate over rows to insert data
    insert_prompt = ''
    for _, row in df.iterrows():
        data = tuple(row)
        insert_prompt += f'INSERT INTO {name} ({", ".join(columns)}) VALUES {data};\n'

    full_prompt = create_table_prompt + insert_prompt
    return full_prompt


if __name__ == '__main__':
    main()
