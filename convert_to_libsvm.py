from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext()
spark = SparkSession(sc)

from pyspark.sql.functions import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, lit
from pyspark.ml.linalg import *

import pandas as pd

# Load features dataframe
# (Output of Feature Generation - Combined Jupyter Notebook)
features = spark.read.csv("features_combined.csv", header=True, inferSchema=True)

# Load notes data
notes = spark.read.csv("notes_raw.csv", header=True, inferSchema=True)

# tokenize notes data
tokenizer = Tokenizer(inputCol="TEXT", outputCol="words")
wordsData = tokenizer.transform(notes)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=100)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# Clean up special characters in column names
features = features.toDF(*(c.replace('.', '_') for c in features.columns))
features = features.toDF(*(c.replace(' ', '_') for c in features.columns))
features = features.toDF(*(c.replace('%', '') for c in features.columns))
features = features.toDF(*(c.replace('/', '') for c in features.columns))
features = features.toDF(*(c.replace('(', '') for c in features.columns))
features = features.toDF(*(c.replace(')', '') for c in features.columns))
features = features.toDF(*(c.replace('*', '') for c in features.columns))
features.printSchema()

# Replace NA values with zero
features = features.fillna(0)

# Merge with NLP data
features_with_nlp = features.join(rescaledData.select("HADM_ID", "features"), on="HADM_ID", how="left")

# Replace empty note rows with an empty vector
from pyspark.ml.linalg import VectorUDT, SparseVector
fill_with_vector = udf(
    lambda x, i: x if x is not None else SparseVector(i, {}),
    VectorUDT()
)
features_with_nlp = features_with_nlp.withColumn("features", fill_with_vector("features", lit(100)))


#####################
# Merge all features to vector representation
inputCols = features_with_nlp.schema.names[2:]
print(inputCols)

assembler = VectorAssembler(
    inputCols=inputCols,
    outputCol="mimic_features")

output = assembler.transform(features_with_nlp)

output.select("mimic_features").show(truncate=False)



# Load list of admissions
# sample = pd.read_csv("sepsis_and_not_sepsis_admissions.csv")
# sample['HADM_ID'] = sample['HADM_ID'].str.extract('([0-9]+)')
# sample_admissions = np.array(sample['HADM_ID']).tolist()
# sample = sample[["HADM_ID", "label"]]
# sample = sqlCtx.createDataFrame(sample)
# sample.write.csv("labels.csv", header=True, mode="overwrite")
sample = spark.read.csv("labels.csv", header=True, inferSchema=True)


# Get ready to save as libsvm
# https://stackoverflow.com/questions/43920111/convert-dataframe-to-libsvm-format
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import DenseVector, VectorUDT

features_libsvm = output.select("HADM_ID", "mimic_features").join(sample, on="HADM_ID", how="left")
features_libsvm = features_libsvm.select("label", "mimic_features")

features_libsvm_rdd = features_libsvm.rdd
features_libsvm_rdd.take(1)

features_libsvm_format = features_libsvm_rdd.map(lambda line: LabeledPoint(line[0],DenseVector(line[1])))
features_libsvm_format.take(3)

MLUtils.saveAsLibSVMFile(features_libsvm_format, "features_combined.libsvm")
