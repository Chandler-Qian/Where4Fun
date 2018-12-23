from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
from time import time

import sys
import boto3
import math
import os
import json
import ast

def recommend(argus):
  sc =SparkContext()

  datasets_path = os.path.join('', 's3n://spsample/test_datasets')

  test_review_datasets_path = os.path.join(datasets_path, 'test_review.json')
  test_business_datasets_path = os.path.join(datasets_path, 'test_business.json')

  test_review_datasets_raw_data = sc.textFile(test_review_datasets_path)
  test_business_datasets_raw_data = sc.textFile(test_business_datasets_path)



  small_ratings_data = test_review_datasets_raw_data.map(lambda line: json.loads(line))\
      .map(lambda line: (line['user_id'],line['business_id'],line['stars'])).cache()

  small_business_data = test_business_datasets_raw_data.map(lambda line: json.loads(line))\
      .map(lambda line: (line['business_id'],line['name'])).cache()

  users = small_ratings_data.map(lambda x: x[0]).distinct().sortBy(lambda x: x).zipWithIndex().collectAsMap()
  business = small_ratings_data.map(lambda x: x[1]).distinct().sortBy(lambda x: x).zipWithIndex().collectAsMap()


  small_ratings_data = small_ratings_data.map(lambda x: (users[x[0]], business[x[1]], x[2]))

  training_RDD, validation_RDD, test_RDD = small_ratings_data.randomSplit([6, 2, 2], seed=0)
  validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
  test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))



  seed = 5
  iterations = 10
  regularization_parameter = 0.1
  ranks = [4, 8, 12]
  errors = [0, 0, 0]
  err = 0
  tolerance = 0.02

  min_error = float('inf')
  best_rank = -1
  best_iteration = -1
  for rank in ranks:
      model = ALS.train(training_RDD, rank, seed=seed, iterations=iterations,
                        lambda_=regularization_parameter)
      predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
      rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
      error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
      errors[err] = error
      err += 1
      print ('For rank %s the RMSE is %s' % (rank, error))
      if error < min_error:
          min_error = error
          best_rank = rank

  print ('The best model was trained with rank %s' % best_rank)


  model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations,
                        lambda_=regularization_parameter)
  predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
  rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
  error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
      
  print ('For testing data the RMSE is %s' % (error))


  complete_ratings_file = os.path.join(datasets_path, 'review.json')
  complete_ratings_raw_data = sc.textFile(complete_ratings_file)

  # Parse
  complete_ratings_data = complete_ratings_raw_data.map(lambda line: json.loads(line))\
      .map(lambda line: (line['user_id'],line['business_id'],line['stars'])).cache()
      
  print ("There are %s recommendations in the complete dataset" % (complete_ratings_data.count()))


  users_all = complete_ratings_data.map(lambda x: x[0]).distinct().sortBy(lambda x: x).zipWithIndex().collectAsMap()
  business_all = complete_ratings_data.map(lambda x: x[1]).distinct().sortBy(lambda x: x).zipWithIndex().collectAsMap()
  complete_ratings_data = complete_ratings_data.map(lambda x: (users_all[x[0]], business_all[x[1]], x[2]))
  # use 

  training_RDD, test_RDD = complete_ratings_data.randomSplit([7, 3], seed=0)

  complete_model = ALS.train(training_RDD, best_rank, seed=seed, 
                             iterations=iterations, lambda_=regularization_parameter)

  test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

  predictions = complete_model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
  rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
  error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
      
  print ('For testing data the RMSE is %s' % (error))

  new_user_ID = -1


  new_user_ratings = argus
  new_user_ratings_RDD = sc.parallelize(new_user_ratings)
  print ('New user ratings: %s' % new_user_ratings_RDD.take(10))

  complete_data_with_new_ratings_RDD = complete_ratings_data.union(new_user_ratings_RDD)


  t0 = time()
  new_ratings_model = ALS.train(complete_data_with_new_ratings_RDD, best_rank, seed=seed, 
                                iterations=iterations, lambda_=regularization_parameter)
  tt = time() - t0

  print ("New model trained in %s seconds" % round(tt,3))

  new_user_ratings_ids = map(lambda x: x[1], new_user_ratings)

  new_user_unrated_ratings_RDD = (complete_ratings_data.filter(lambda x: x[0] not in new_user_ratings_ids).map(lambda x: (new_user_ID, x[0])))


  new_user_recommendations_RDD = new_ratings_model.predictAll(new_user_unrated_ratings_RDD)


  new_user_recommendations_rating_RDD = new_user_recommendations_RDD.map(lambda x: (x.product, x.rating))


  business_datasets_path = os.path.join(datasets_path, 'business.json')
  business_datasets_raw_data = sc.textFile(business_datasets_path)
  business_data = business_datasets_raw_data.map(lambda line: json.loads(line))\
    .map(lambda line: (line['business_id'],line['name'],str(line['latitude']),str(line['longitude']),line['categories'])).cache()

  indexId = new_user_recommendations_rating_RDD.collect()[0][0]

  reversed_dictionary = dict(map(reversed, business.items()))
  bus_id = reversed_dictionary[indexId]

  bus_dic = {}
  for i,j,k,l,m in business_data.collect():
    bus_dic[i] = (j,k,l,m)



  output = new_user_recommendations_rating_RDD.map(lambda x: (bus_dic.get(reversed_dictionary.get(x[0])),x[1])).takeOrdered(20, key=lambda x: -x[1])

  s = str(set(output))



  s3 = boto3.resource('s3')
  object = s3.Object('spsample', 'output.txt')
  object.put(Body=s)                                                   

if __name__== "__main__":
    arr = ast.literal_eval(sys.argv[1])
    recommend(arr)




















