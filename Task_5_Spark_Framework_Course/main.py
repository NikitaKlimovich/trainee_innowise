import warnings

import findspark
from create_dataframes import connect, create_tables
from run_queries import get_result


findspark.init()
warnings.filterwarnings("ignore")


if __name__ == "__main__":
    conn = connect()
    tables = create_tables(conn)
    get_result(tables)
