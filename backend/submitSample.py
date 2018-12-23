from __future__ import print_function
import json
import boto3
#import datetime

def lambda_handler(event, context):
    # TODO implement
    print(event)
    args = str(event["args"])
    emr = boto3.client("emr")
    # clusters = emr.list_clusters()
    # clusters = [c["Id"] for c in clusters["Clusters"] if c["Status"]["State"] in ["RUNNING","WAITTING"]]
    cluster_id = 'j-37LMBVYQ3AO2W'
    CODE_DIR = '/home/hadoop/code/'
    step_args = ["/usr/bin/spark-submit", CODE_DIR + "sample.py" , args]
    response = emr.add_job_flow_steps(
        JobFlowId = cluster_id,
        Steps = [
            {
              'Name':'run sample.py',# + str(datetime.datetime.now()),
              'ActionOnFailure' : 'CONTINUE',
              'HadoopJarStep' : {
                  'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
                  'Args' : step_args
              }
            },
        ]
    )
    # output = [(('Michoacan Auto', '36.1490011845', '-115.105679221', 'Tires, Automotive, Auto Repair'), 4.924599349511782), (('Sorrento Ristorante & Pizzeria', '41.4269951', '-82.080759', 'Italian, Pizza, Beer, Wine & Spirits, Food, Restaurants'), 5.507319425493703), (('Shish Kabob House', '36.1150513', '-115.2365638', 'Middle Eastern, Restaurants, Mediterranean'), 5.471982554179271), (None, 5.503893506393702), (None, 5.280090422879562), (('Central Church - Henderson', '36.0804531', '-115.0381656', 'Churches, Religious Organizations'), 5.112925906476242), (('Brilliant Bridal', '36.1452494', '-115.1795498', 'Shopping, Bridal'), 5.024789641662957), (('The Bier Markt', '43.6470951613', '-79.37391518', 'Bars, Belgian, Food, Beer, Wine & Spirits, Canadian (New), Gastropubs, Restaurants, Nightlife'), 5.820852285250083), (('Caveman Burgers', '33.6411816', '-112.0681534', 'Restaurants, Burgers'), 5.516510635643468), (('Hakka Wow', '43.672085', '-79.3220862', 'Restaurants, Chinese, Hakka'), 5.440234101011096)]
    # return [(('Michoacan Auto', '36.1490011845', '-115.105679221', 'Tires, Automotive, Auto Repair'), 4.924599349511782), (('Sorrento Ristorante & Pizzeria', '41.4269951', '-82.080759', 'Italian, Pizza, Beer, Wine & Spirits, Food, Restaurants'), 5.507319425493703), (('Shish Kabob House', '36.1150513', '-115.2365638', 'Middle Eastern, Restaurants, Mediterranean'), 5.471982554179271), (None, 5.503893506393702), (None, 5.280090422879562), (('Central Church - Henderson', '36.0804531', '-115.0381656', 'Churches, Religious Organizations'), 5.112925906476242), (('Brilliant Bridal', '36.1452494', '-115.1795498', 'Shopping, Bridal'), 5.024789641662957), (('The Bier Markt', '43.6470951613', '-79.37391518', 'Bars, Belgian, Food, Beer, Wine & Spirits, Canadian (New), Gastropubs, Restaurants, Nightlife'), 5.820852285250083), (('Caveman Burgers', '33.6411816', '-112.0681534', 'Restaurants, Burgers'), 5.516510635643468), (('Hakka Wow', '43.672085', '-79.3220862', 'Restaurants, Chinese, Hakka'), 5.440234101011096)]
    
    # return clusters
    
    # {
    #     # 'statusCode': 200,
    #     # 'body': json.dumps('Hello from Lambda!')
    # }
