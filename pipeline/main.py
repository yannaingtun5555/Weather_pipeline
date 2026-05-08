from pdb import run

from extract import extract
from transform import transform_data
from load import load_data
from table_create import create_table

def run_pipeline():
    if not create_table():
        print("Error ")
    data = extract()
    res = transform_data(data)
    load_data(res)

if __name__ == __main__:
    run_pipeline()