from prefect import Flow, task
from codema_drem.preprocess.BER import run_ber_etl_flow
from codema_drem.preprocess.Census2016 import run_census_etl_flow
# from codema_drem.preprocess.Dublin_boundary import run_census_etl_flow
# from codema_drem.preprocess.Census2016 import run_census_etl_flow

if __name__ == '__main__':

    with Flow('Run model') as flow:

        # PREPROCESS DATA
