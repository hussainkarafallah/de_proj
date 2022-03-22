from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import argparse 
from pyspark.ml.feature import Tokenizer , HashingTF , IDF ,StopWordsRemover
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
import time

parser = argparse.ArgumentParser()
parser.add_argument("--data" , type = str , action = 'store' , help = "path to files in hdfs" , required = True)
parser.add_argument("-text" , "--text_field" , action = 'store' ,  help = "name of the text field in json, no need to specify if using reddit data")
parser.add_argument("-label" , "--label_field" , action = 'store'  , help = "name of the label field in json, no need to specify if using reddit data")
parser.add_argument("--other" , action = 'store_true' , default=False , help = "set this flag if you use other source than reddit to skip preprocessing")
parser.add_argument("--save" , action = 'store' , type = str , default = "/trained" , help = "path to save the model")
parser.add_argument("--cores" , action = 'store' , type = int , default = 2 , help = "number of cores")

args = parser.parse_args()
print(args)

spark = (SparkSession.builder
                     .master('yarn')
                     .appName('train')
                     .config('spark.executor.cores' , args.cores)
                     .getOrCreate()
        )

data_path = 'hdfs://group9-master:9000{}'.format(args.data)
print("Reading data from {}".format(data_path))
df = spark.read.json(data_path)


if not args.other:
    df = df.select('author' , 'body' , 'score')

    df = df.where(
        df.body != '[deleted]'
    )

    df = df.withColumn(
        'label',
        (df.score < -5)
    )

    df = df.withColumn(
        'label',
        df.label.cast(IntegerType())
    )

    TEXT_FIELD = 'body'
    LABEL_FIELD = 'label'

else:
    TEXT_FIELD = args.text_field
    LABEL_FIELD = args.label_field
    df = df.select(args.text_field , args.label_field)

total_entries = df.count()
print("Total entries :: {}".format(total_entries))

tokenizer = Tokenizer(inputCol=TEXT_FIELD, outputCol="words")
remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol='words_clean')
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="wordCounts")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
model = NaiveBayes(featuresCol = idf.getOutputCol() , predictionCol = 'prediction' , labelCol = LABEL_FIELD)
pipeline = Pipeline(stages=[tokenizer, remover ,  hashingTF, idf , model])


st_time = time.time()
model = pipeline.fit(df)
en_time = time.time()

print("Total training time :: {}".format(en_time - st_time))
prediction = model.transform(df)


accuracy= (
            prediction.where( prediction.label == prediction.prediction ).count() /
            total_entries
)

precision = (
            prediction.where(prediction.label == 1)
                      .where(prediction.label == prediction.prediction).count() /
            prediction.where(prediction.label == 1).count() 
)

recall = (
            prediction.where(prediction.label == 0)
                      .where(prediction.label == prediction.prediction).count() /
            prediction.where(prediction.label == 0).count() 
)



print("Total Training Accuracy :: {}".format(accuracy))
print("Total Training Precision :: {}".format(precision))
print("Total Training Recall :: {}".format(recall))

save_path = 'hdfs://group9-master:9000{}'.format(args.save)
model.write().overwrite().save(save_path)
