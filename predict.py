from pyspark.sql import SparkSession
from pyspark.ml.pipeline import PipelineModel
import argparse
import time 

parser = argparse.ArgumentParser()
parser.add_argument("--data" , type = str , action = 'store' , help = "path to files in hdfs" , required = True)
parser.add_argument("--model" , type = str , action = 'store' , help = "path to the saved model" , required = True)
parser.add_argument("--results" , action = 'store' , type = str , default = "/trained" , help = "path to save predictions")
parser.add_argument("-text" , "--text_field" , action = 'store' ,  help = "name of the text field in json, no need to specify if using reddit data")
parser.add_argument("--cores" , action = 'store' , type = int , default = 2 , help = "number of cores")

args = parser.parse_args()

spark = (SparkSession.builder
                     .master('yarn')
                     .appName('predict')
                     .config('spark.executor.cores' , args.cores)
                     .getOrCreate()
        )
        
data_path = 'hdfs://group9-master:9000{}'.format(args.data)
df = spark.read.json(data_path)

st_time = time.time()

model_path = 'hdfs://group9-master:9000{}'.format(args.model)
persisted_model = PipelineModel.load(model_path)
tokenizer = persisted_model.stages[0]
tokenizer.setInputCol(args.text_field)

results_path = 'hdfs://group9-master:9000{}'.format(args.results)
results = persisted_model.transform(df).select('prediction')
results.write.csv(args.results)

en_time = time.time()

print("Total elapsed time :: {}".format(en_time - st_time))


