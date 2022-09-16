from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

import boto3
import botocore
import awswrangler as wr
import json
import os
import jinja2 as j2
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

LOGGER = logging.getLogger()

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SQL_SCRIPTS_PATH = os.path.join(DIR_PATH,"sql_jobs")

J2_ENV = j2.Environment(loader=j2.FileSystemLoader(SQL_SCRIPTS_PATH), undefined=j2.StrictUndefined)

def get_query(sql_script:str, params: Dict[str, Any] = None):

    if params is None:
        params = {}
    query = J2_ENV.get_template(sql_script).render(**params)
    LOGGER.info(f'*****RETRIEVED QUERY*****\n{query}')
    return query

@dataclass
class MaterializeAthenaQuery:
    """MaterializeAthenaQuery allows the user to run and publish athena Queries
    Args:
        sql_query_path (str): path for sql in the sql jobs directory
        target_bucket (str): bucket where data will be stored
        target_database (str): glue db to store data in
        target_table (str): table to store data in
        table_description (str): description of resulting table
        stg_athena_bucket (str): temp location where Athena results are stored
        partition_cols (List[str], optional): columns to parition resulting table by
        query_params (Dict, , optional): dict of parmeters to pass into jinja templae
        savemode (str): not in the constructor, how the query is saved
    """
    sql_query_path: str
    target_bucket: str
    target_database: str
    target_table: str
    table_description: str
    stg_athena_bucket: str
    partition_cols: List[str] = field(default_factory=list)
    query_params: Dict = field(default_factory=dict)
    savemode: str = field(init=False)
    # dtypes: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        self.savemode = "overwrite_partitions" if self.partition_cols else "overwrite"

    def process_query(self) -> Optional[str]:
        try:
            LOGGER.info(f"Materialize Athena Query request with arguments:\n"
            f"\tSQL_QUERY_PATH ::: {self.sql_query_path}\n"
            f"\tTARGET_BUCKET ::: {self.target_bucket}\n"
            f"\tTARGET_DB ::: {self.target_database}\n"
            f"\tTARGET_TABLE ::: {self.target_table}\n"
            f"\tTABLE_DESC ::: {self.table_description}\n"
            f"\tSTG_ATHENA_BUCKET ::: {self.stg_athena_bucket}\n"
            f"\tPARTITION_COLS ::: {str(self.partition_cols)}\n"
            f"\tQUERY_PARAMS ::: {str(self.query_params)}\n"
            f"\tSAVEMODE ::: {self.savemode}" )

            sql_query = get_query(self.sql_query_path,self.query_params)

            wr.config.botocore_config = botocore.config.Config(
                retries={"max_attempts": 5},
                connect_timeout=10,
                max_pool_connections=10,
                read_timeout=900 # 15 mins read timeout
            )

            # run query and return as dataframe
            df = wr.athena.read_sql_query(
                sql=sql_query,
                database=self.target_database, 
                ctas_approach=False, # see different approaches here https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.athena.read_sql_query.html
                s3_output=f"s3://{self.stg_athena_bucket}/{self.target_table}"
            )

            LOGGER.info(
                '*****Query Stats*****\n%s',
                json.dumps(df.query_metadata["Statistics"], sort_keys=True, indent=2),
            )

            s3_dataset_output = f"s3://{self.target_bucket}/{self.target_database}/{self.target_table}"
            LOGGER.info(f'*****OUTPUT S3 TARGET*****\n\t {s3_dataset_output}')

            # write the dataframe to the destination
            write_result = wr.s3.to_parquet(
                df=df,
                path=s3_dataset_output,
                dataset=True,
                database=self.target_database,
                table=self.target_table,
                mode=self.savemode,
                partition_cols=self.partition_cols,
                schema_evolution=True,
                index=False,
                description=self.table_description
                # dtype=self.dtypes
            )
            LOGGER.info(
                '*****RESULT*****\n%s',
                json.dumps(write_result, sort_keys=True, indent=2),
            )
        except Exception as exc:
            LOGGER.exception(exc)

def main(argv):
    if len(argv)!=7:
        LOGGER.info("Syntax: python run_athena_query.py <<sql_query_path>> <<target_bucket>> <<target_database>> <<target_table>> <<target_description>> <<stg_athena_bucket>>")
    
    LOGGER.info(f"Received {len(argv)} arguments:\n"
        f"\tSQL Query Path ::: {argv[1]}\n"
        f"\tTarget Bucket ::: {argv[2]}\n"
        f"\tTarget Database ::: {argv[3]}\n"
        f"\tTarget Table ::: {argv[4]}\n"
        f"\tTarget Description ::: {argv[5]}\n"
        f"\tSTG/tmp Athena Results bucket ::: {argv[6]}" )
    req = MaterializeAthenaQuery(sql_query_path=argv[1],
        target_bucket=argv[2],
        target_database=argv[3],
        target_table=argv[4],
        table_description=argv[5],
        stg_athena_bucket=argv[6]
    )

    # req = MaterializeAthenaQuery(sql_query_path="some_project/sample-nyc-covid.sql",
    # target_bucket="ryagomes-qs-source",
    # target_database="processed_covid",
    # target_table="nyc_covid_data",
    # table_description="Processed covid data for NYC",
    # stg_athena_bucket="ryagomes-covid-test")
    req.process_query()


if __name__ == '__main__':
	main(sys.argv)
