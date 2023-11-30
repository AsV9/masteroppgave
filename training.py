import pandas as pd
import h2o
from h2o.estimators import H2ORandomForestEstimator

#initialize the H2O cluster
h2o.init()


#load the data with pandas
pdata = pd.read_csv("data/behaviours/lying.csv")

#remove cols "time", "serial", "weight" from the pandas dataframe
pdata = pdata.drop(["time", "serial"], axis=1)

#convert the pandas dataframe to an H2O dataframe
data = h2o.H2OFrame(pdata)

train, test = data.split_frame(ratios=[.8])

# Identify predictors and response
x = train.columns
y = "lying"  # Replace with target column
x.remove(y)

# Convert the response column to a factor for classification
train[y] = train[y].asfactor()
test[y] = test[y].asfactor()

test_depth = [0]

for i in test_depth:
    # Initialize and train the Random Forest model with class weights
    rf_model = H2ORandomForestEstimator(ntrees=200, max_depth=i, seed=1234, balance_classes=True, binomial_double_trees=True, mtries=-1)
    rf_model.train(x=x, y=y, training_frame=train)

    # Evaluate model performance
    performance = rf_model.model_performance(test_data=test)
    print(performance)

    with open("data/behaviours/lying_rf_model_performance_maxdepth" + (str(i)) + ".txt", "w") as text_file:
        text_file.write(str(performance))