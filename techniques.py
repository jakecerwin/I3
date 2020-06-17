import pandas as pd
import numpy as np

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.metrics import median_absolute_error

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.layers import Dense

from sklearn.preprocessing import StandardScaler

import pickle
import time


def nn_shallow(X_train, y_train, X_test, y_test):
    model = keras.Sequential()

    # Add an input layer
    model.add(Dense(36, activation=tf.nn.relu, input_shape=(36,)))

    # Add one hidden layer
    model.add(Dense(18, activation=tf.nn.relu))

    # Add an output layer
    model.add(Dense(1))

    # Compile the model
    model.compile(loss='mean_squared_error',
                  optimizer=tf.optimizers.Adam())

    start = time.time()

    model.fit(X_train, y_train, epochs=40, batch_size=32, verbose=0)
    training_cost = time.time() - start

    start = time.time()
    y_pred = model.predict(X_test)
    #print(y_pred)
    inference_cost = (time.time() - start) * 50000 / len(y_pred)

    # accuracy operationalized
    mse = mean_squared_error(y_test, y_pred, squared=True)
    mae = median_absolute_error(y_test, y_pred)

    model_json = model.to_json()
    with open("small_model.json", "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    model.save_weights("small_model.h5")


    return {'name': 'quick regression', "training  cost: ": training_cost, "inference cost: ": inference_cost,
            "mse": mse, "mae": mae}



def nn_long(X_train, y_train, X_test, y_test):
    model = keras.Sequential()

    # Add an input layer
    model.add(Dense(36, activation=tf.nn.relu, input_shape=(36,)))

    # Add 4 hidden layers
    model.add(Dense(36, activation=tf.nn.relu))
    model.add(Dense(36, activation=tf.nn.relu))
    model.add(Dense(36, activation=tf.nn.relu))
    model.add(Dense(36, activation=tf.nn.relu))


    # Add an output layer
    model.add(Dense(1),)

    # Compile the model
    model.compile(loss='mean_squared_error',
                  optimizer=tf.optimizers.Adam())

    start = time.time()

    model.fit(X_train, y_train, epochs=20, batch_size=32, verbose=0)
    training_cost = time.time() - start

    start = time.time()
    y_pred = model.predict(X_test)

    inference_cost = (time.time() - start) * 50000 / len(y_pred)

    # accuracy operationalized
    mse = mean_squared_error(y_test, y_pred, squared=True)
    mae = median_absolute_error(y_test, y_pred)

    model_json = model.to_json()
    with open("large_model.json", "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    model.save_weights("large_model.h5")


    return {'name': 'long nn regression', "training  cost: ": training_cost, "inference cost: ": inference_cost,
            "mse": mse, "mae": mae}




# random forest regression
def rf_regression(X_train, y_train, X_test, y_test):
    # Training cost operationalized
    start = time.time()
    reg = RandomForestRegressor(random_state=0, max_depth=10)
    reg.fit(X_train, y_train)
    training_cost = time.time() - start

    # inference ost operationalized
    start = time.time()
    y_pred = reg.predict(X_test)
    inference_cost = (time.time() - start) * 50000 / len(y_pred)

    # accuracy operationalized
    mse = mean_squared_error(y_test, y_pred, squared=True)
    mae = median_absolute_error(y_test, y_pred)

    #within = {1:0,2:0,3:0,4:0,5:0,6:0,7:0,8:0,9:0,10:0}
    #for i in range(len(y_pred)):
    #    error = abs(y_pred[i] - y_test[i])
    #    for j in range(10):
    #        if error < j+1:
    #            within[j+1] += 1

    #for key in within:
    #    within[key] = within[key] / len(y_pred)

    #import matplotlib.pylab as plt

    #lists = sorted(within.items())  # sorted by key, return a list of tuples

    #x, y = zip(*lists)  # unpack a list of pairs into two tuples

    #plt.plot(x, y)
    #plt.show()


    pickle.dump(reg, open("rf1.sav", 'wb'))


    return {'name': 'rf regression', "training  cost: ": training_cost, "inference cost: ": inference_cost,
            "mse": mse, "mae": mae}



def heat_map(data):
    import seaborn as sns
    import matplotlib.pyplot as plt

    corr = movie_data.corr()
    sns.heatmap(corr,
            xticklabels=corr.columns.values,
            yticklabels=corr.columns.values)
    plt.show()



# Prepare data into X_train, X_test, y_train, y_test
movie_data = pd.read_csv('movie_data.csv')
#print(movie_data.info())
#heat_map(movie_data)

# Specify the data (feature matrix)

X = movie_data.iloc[:, :-2]

# Specify the target labels and flatten the array
y = np.ravel(movie_data.views)


# Split the data up in train and test sets
# random_state controls the shuffling, allows for reproducible output
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

#rf1 = rf_regression(X_train, y_train, X_test, y_test)

# Define the scaler
scalerX = StandardScaler().fit(X_train)

# Scale the train set
X_train = scalerX.transform(X_train)

# Scale the test set
X_test = scalerX.transform(X_test)

rf2 = rf_regression(X_train, y_train, X_test, y_test)

nn1 = nn_shallow(X_train, y_train, X_test, y_test)
nn2 = nn_long(X_train, y_train, X_test, y_test)

#print(rf1)
print(rf2)
print(nn1)
print(nn2)
