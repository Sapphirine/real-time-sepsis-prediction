# real-time-sepsis-prediction
E6893 Final Project for egm2143, djf2150, and kr2741

## Environment
Our development environment was an Amazon Web Services EMR cluster configured with Hadoop, Spark, and Jupyter Notebooks server, as described in this blog post: 
https://aws.amazon.com/blogs/big-data/running-jupyter-notebook-and-jupyterhub-on-amazon-emr/


## Data

### MIMIC-III Data
We used the MIMIC-III dataset, which is publically available with proper authorization. 

Learn more about the data set and requst access here: 
https://mimic.physionet.org/

#### Data Preparation
We loaded the MIMIC CSV files into an Amazon S3 bucket. Our code assumes that the MIMIC csv files are in an S3 bucket named `mimic-raw`, and that each csv is stored in a folder of the same name. The code assumes mimic CSVs in the following s3 locations. If your CSVs are in a different location, you may need to update pieces of the code. 

    s3://mimic-raw/mimic3/admissions/ADMISSIONS.csv
    s3://mimic-raw/mimic3/prescriptions/PRESCRIPTIONS.csv
    s3://mimic-raw/mimic3/cptevents/CPTEVENTS.csv
    s3://mimic-raw/mimic3/labevents/LABEVENTS.csv
    s3://mimic-raw/mimic3/d_labitems/D_LABITEMS.csv
    s3://mimic-raw/mimic3/noteevents/NOTEEVENTS.csv
    s3://mimic-raw/mimic3/noteevents/updated-note.csv

## Feature Engineering

#### Sepsis Labels.ipynb

Since there is no explicit field in MIMIC to say whether a patient did or did not have sepsis during their admission, we had to extract the label information. 

We coded cases as positive for sepsis if they had one of the explicit ICD-9 Diagnosis Codes for sepsis:
* 995.92 (severe sepsis) 
* 785.52 (septic shock)

These codes are extremely specific to sepsis, but have very low sensitivity. From Iwashyna et al. (vs. chart reviews): 100% PPV, 9.3% sens, 100% specificity.

For negative cases we used the definition of sepsis from Angus et al, 2001. Epidemiology of severe sepsis in the United States (http://www.ncbi.nlm.nih.gov/pubmed/11445675) as a counterfactual and selected all acute care hospitalizations with ICD-9-CM codes for neither:

* bacterial or fungal infectious process 
nor
* diagnosis of acute organ dysfunction


#### Feature Engeneering - Combined.ipynb

To construct our feature vectors, we extracted some general patient and admission information, as well as counts of key clinical activities like medication orders, lab orders, and procedures. We limited features to only those within 24 hours after admission. 

We also extracted word token features from clinical notes within the first 24 hours of admission. 

We limited features to the 100 most common labs, 100 most common medications, 50 most common CPT procedures, and 100 most common word tokens. This helped to limit our feature space. 

We save the feature matrix to the HDFS as we go. 

Output: `features_combined.csv` saved to the HDFS

#### convert_to_libsvm.py
Due to limitations with out Jupyter environment setup, we created a separate pyspark script to convert our Admission by Feature matrix into the `libsvm` format required by Spark's ML library.

Output: `features_combined.libsvm/` saved to the HDFS

## Prediction

### Classification.ipynb
Our classification experiments are stored in `Classification.ipynb`. We experimented with Logistic Regression, Decision Tree, Gradient Boosted Tree, Random Forrest, and Naive Bayes algorithms. 


## Visualization for clinicians

### visualization/interactive_visualization
This R script uses the ggplot2 and plotly packages to create an interactive visualiation of results from our model.
