import sqlite3
from pandas import DataFrame
import pandas as pd
import logging
pd.set_option("display.max_columns", 10)


logging.basicConfig(level=logging.DEBUG,
                    filename="bench.log",
                    filemode="w")

logging.info("Start program")


conn = sqlite3.connect('TestDB.db')
c = conn.cursor()
conn.commit()
df = pd.read_csv("annual-enterprise-survey-2019-financial-year-provisional-csv.csv")
df.to_sql("Survey", conn, if_exists='replace', index=False)


def benchmark(func):
    import time

    def wrapper(*a, **b):
        start = time.time()
        res = func(*a, **b)
        end = time.time()
        logging.info('[*] Время выполнения: {} секунд.'.format(end - start))
        return res

    return wrapper

@benchmark
def indust_2015():
    c.execute("""
    SELECT Year, Industry_name_NZSIOC, Units, Value FROM Survey
    WHERE Year = 2015 AND Industry_name_NZSIOC = "All industries" AND Units = "Dollars (millions)"
    AND Value != "C" AND Value != "S"
            """)

    df = DataFrame(c.fetchall(), columns=['Year', 'Industry_name_NZSIOC', 'Units', 'Value'])
    print(df)

@benchmark
def val_gambling():
    c.execute("""
    SELECT Industry_name_NZSIOC, Value, Units FROM Survey
    WHERE Industry_name_NZSIOC = "Gambling Activities" AND Units = "Dollars (millions)" AND Value != "C" AND Value != "S"
            """)
    df = DataFrame(c.fetchall(), columns=['Industry_name_NZSIOC', 'Value', 'Units'])
    print(df)

@benchmark
def salaries_wages():
    c.execute("""
    SELECT Year, Industry_aggregation_NZSIOC, Variable_name, Value, Units FROM Survey
    WHERE Year = "2019" AND Industry_aggregation_NZSIOC = "Level 1" AND Variable_name = "Salaries and wages paid"
    AND Units = "Dollars (millions)" AND Value != "C" AND Value != "S"
            """)
    df = DataFrame(c.fetchall(), columns=['Year', 'Industry_aggregation_NZSIOC', 'Variable_name',
                                          'Value', 'Units'])
    print(df)




@benchmark
def df_indust_2015():
    d_f = df.drop(columns=["Industry_aggregation_NZSIOC", "Industry_code_NZSIOC", "Variable_code", "Variable_category",
                     "Industry_code_ANZSIC06", 'Variable_name'], axis=1, inplace=False)
    print(d_f[(df['Value'] != "C") & (df['Year'] == 2015) & (df['Value'] != "S") &
             (df['Industry_name_NZSIOC'] == "All industries") & (df['Units'] == "Dollars (millions)")])

@benchmark
def df_val_gambling():
    d_f = df.drop(columns=["Industry_aggregation_NZSIOC", "Industry_code_NZSIOC", "Variable_code", "Variable_category",
                     "Industry_code_ANZSIC06", 'Variable_name', 'Year'], axis=1, inplace=False)
    print(d_f[(df['Value'] != "C") & (df['Value'] != "S") &
             (df['Industry_name_NZSIOC'] == "Gambling Activities") & (df['Units'] == "Dollars (millions)")])

@benchmark
def df_salaries_wages():
    d_f = df.drop(columns=["Industry_name_NZSIOC", "Industry_code_NZSIOC", "Variable_code", "Variable_category",
                     "Industry_code_ANZSIC06" ], axis=1, inplace=False)
    print(d_f[(df['Value'] != "C") & (df['Year'] == 2019) & (df['Value'] != "S") &
             (df['Industry_aggregation_NZSIOC'] == "Level 1") & (df['Variable_name'] == "Salaries and wages paid") &
             (df['Units'] == "Dollars (millions)")])


indust_2015()
val_gambling()
salaries_wages()
df_indust_2015()
df_val_gambling()
df_salaries_wages()

logging.info("End of the program")