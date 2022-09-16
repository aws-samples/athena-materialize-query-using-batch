import sys
# import os
# from urllib.parse import urlparse
# p1 = 's3://ryagomes-qs-source/covid_state_data/'
# p2 = 's3://covid19-lake/rearc-covid-19-nyt-data-in-usa/json/us-counties'

# def pp(p2):
#     loc = urlparse(p2)
    
#     bucket=loc.netloc
#     path=loc.path[1:]
#     print(f'bucket:: {bucket}')
#     print(f'path:: {path}')

#     db_arn=f'arn:aws:s3:::{bucket}'

#     folder=os.path.join(db_arn,f"{path}_$folder$")
#     star=os.path.join(db_arn, path, "*")
#     print(f'1. db_arn:: {db_arn}')
#     print(f'2. folder:: {folder}')
#     print(f'2. star:: {star}')
    
# pp(p1)

print(sys.argv)