#!/usr/bin/env python
# coding: utf-8

# In[9]:


import sys
import os
import time
import pickle
from datetime import datetime, timedelta, timezone
import boto3
import pandas as pd

from settings import Settings

s = Settings()

AWS_ACCESS_KEY_ID = s.key['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = s.key['AWS_SECRET_ACCESS_KEY']
IS_SANDBOX = s.key['IS_SANDBOX']


# In[10]:


def get_client(id, key, is_sandbox):

    if is_sandbox:
        return boto3.client("mturk",
                            aws_access_key_id = id,         
                            aws_secret_access_key = key,
                            region_name = "us-east-1",
                            endpoint_url = "https://mturk-requester-sandbox.us-east-1.amazonaws.com")
    else:
        return boto3.client("mturk",
                            aws_access_key_id = id,         
                            aws_secret_access_key = key,
                            region_name = "us-east-1",
                            endpoint_url = "https://mturk-requester.us-east-1.amazonaws.com")


# In[4]:


def throw_a_hit(task_settings:dict):
    print('Remaining Balance: '
          +client.get_account_balance()['AvailableBalance'])
    
    if float(client.get_account_balance()['AvailableBalance']) <= 2:
        sys.exit(" ※Can't create a HIT!!!※")

    # create output directory
    tz = timezone(timedelta(hours=+9), 'Asia/Tokyo')
    dt = datetime.now(tz)
    current_time = str(dt.strftime('%Y_%m%d_%H%M_%S')) # YYYY_MMDD_HHMM_SS
    output_path = './Output/' + current_time
    os.makedirs(output_path)


    # hit creation
    ts = task_settings
    with open("./Tasks"+ts['TASK_FILENAME']) as f:
        res = client.create_hit(
            Title= ts['TASK_TITLE'],
            Description=ts['DESCRIPTION'],
            Keywords=ts['KEYWORDS'],    
            Reward=ts['REWARD'],
            MaxAssignments=ts['MAX_ASSIGNMENTS'],   
            LifetimeInSeconds=ts['LIFETIME_IN_SECONDS'], 
            AssignmentDurationInSeconds=ts['ASSIGNMENT_DURATION_IN_SECONDS'], 
            AutoApprovalDelayInSeconds=ts['APPROVAL_DELAY_IN_SECONDS'],     
            Question=f.read()
        )

    # verbose
    print(' [CREATED A NEW HIT!]')
    hit_id = res["HIT"]["HITId"]
    print(" HIT ID:", res["HIT"]["HITId"])
    print(" Status Code:", res["ResponseMetadata"]['HTTPStatusCode'])
    print(" Creation Time:", res["ResponseMetadata"]['HTTPHeaders']['date'])



    # check status of worker responses once every 10sec
    columns = ['date', 'N_Pending','N_Available','N_Finished','N_Completed']
    data = pd.DataFrame(columns = columns)

    for i in range(s.record['STATUS_CHECK_NUM']):
        res = client.get_hit(HITId=hit_id)
        n_pending = res['HIT']['NumberOfAssignmentsPending']
        n_available = res['HIT']['NumberOfAssignmentsAvailable']
        n_finished =  ts['MAX_ASSIGNMENTS'] - (n_pending + n_available)
        n_completed = res['HIT']['NumberOfAssignmentsCompleted']
        date = pd.to_datetime(res['ResponseMetadata']['HTTPHeaders']['date'])
        dict = {'date':date,
                'seconds_passed':int(i*10),
                'N_Pending':n_pending,
                'N_Available':n_available,
                'N_Finished':n_finished,
                'N_Completed':n_completed}
        data = data.append(dict, ignore_index=True) 
        print(' '+str(dict['seconds_passed'])+'s, '
              +'Pending: '+str(dict['N_Pending'])
              +', Finished: '+str(dict['N_Finished']))
        time.sleep(10)

    # Saving
    data.to_csv(output_path+'/transition_data.csv')
    with open(output_path+'/res_creation.json', 'wb') as fp:
        pickle.dump(res,fp)
    with open(output_path+'/task_config.json', 'wb') as fp:
        pickle.dump(ts,fp)
    with open(output_path+'/hit_details_final.json', 'wb') as fp:
        pickle.dump(client.get_hit(HITId=hit_id),fp)
    with open(output_path+'/list_assignments_for_hit.json', 'wb') as fp:
        pickle.dump(client.list_assignments_for_hit(HITId=hit_id),fp)

    return hit_id


# In[12]:


client = get_client(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, IS_SANDBOX)
client.get_account_balance()['AvailableBalance']


# In[10]:


if __name__ == '__main__':
    print("Start")
    for i in range(6):
        

        while True:
            if datetime.now().minute != 00:
                time.sleep(58)
                continue
            
            print(str(i+1)+"回目の依頼を開始")
            print(datetime.now())
            hit_id = throw_a_hit()
            print(str(i+1)+"回目の依頼が完了")
            break
        
        time.sleep(60)
    print("プログラム終了")


# In[ ]:





# In[5]:


# # HITの停止
# hit_id = '3T2HW4QDV3VNPGKDBOYG8WB7WCC9C3'
# res = client.update_expiration_for_hit(HITId=hit_id, ExpireAt=datetime(1,1,1)) # 現在より前
# print(res["ResponseMetadata"]["HTTPStatusCode"])


# In[7]:


#-------------------run-only-in-ipynb-environment-------------------------------

# ※CAUTION※ Save this file before executing the following code!!!
# Generate py from ipynb and save it automatically

if 'get_ipython' in globals():
    import subprocess
    subprocess.run(['jupyter', 'nbconvert', '--to', 'python', '*.ipynb'])
    print('Saved!')
# End of if 'if 'get_ipython' in globals():'

#-------------------run-only-in-ipynb-environment-------------------------------

