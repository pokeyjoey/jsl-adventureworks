import pandas as pd
import psycopg2
import warnings

warnings.filterwarnings('ignore')

conn = psycopg2.connect(database="adventureworks", user="postgres")
