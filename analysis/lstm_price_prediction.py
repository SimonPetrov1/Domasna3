import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score
import tensorflow as tf
from tensorflow import keras

#-------------------CONFIG----------------------
COIN_SYMBOL = "BTC"     # Choose what coin you want to search
LOOKBACK = 30           # days to lookback
TRAIN_RATIO = 0.7       # train set percentage
EPOCHS = 20
BATCH_SIZE = 32

#-----------------LOAD DATA---------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
DATA_PATH = os.path.join(PROJECT_ROOT, "techPrototype", "../data/processed/all_coins.csv")

print(f"[-INFO-] Loading data from:  {DATA_PATH}")
df = pd.read_csv(DATA_PATH)

coin_df = df[df["symbol"]==COIN_SYMBOL].sort_values("time").copy()

if coin_df.empty:
    raise ValueError(f"[-ERROR-] No rows found for symbol {COIN_SYMBOL} in all_coins.csv")

prices = coin_df["close"].values.reshape(-1, 1)
print(f"[-INFO-] Loaded {len(prices)} price points for {COIN_SYMBOL}")

#------------------SCALE DATA--------------------
scaler = MinMaxScaler(feature_range =(0, 1))
prices_scaled = scaler.fit_transform(prices)

#------------------CREATE SEQUENCES--------------
def create_sequences(data: np.ndarray, lookback: int):
    X, y = [], []
    for i in range(len(data) - lookback):
        X.append(data[i : i + lookback])
        y.append(data[i+lookback])
    return np.array(X), np.array(y)

X, y = create_sequences(prices_scaled, lookback=LOOKBACK)
print(f"[-INFO-] X shape: {X.shape}  (samples, timesteps, features)")
print(f"[-INFO-] y shape: {y.shape}  (samples, 1)")

#------------------TRAIN / TEST SPLIT------------
train_size = int(len(X) * TRAIN_RATIO)
X_train, X_test = X[:train_size], X[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

print(f"[-INFO-] Train samples: {len(X_train)}")
print(f"[-INFO-] Test samples:  {len(X_test)}")

if len(X_test) == 0:
    raise ValueError("[-ERROR-] Not enough data for the chosen LOOKBACK/TRAIN_RATIO.")

#--------------BUILD LSTM MODEL------------------
model = keras.Sequential()
model.add(
    keras.layers.LSTM(
        units=50,
        activation="tanh",
        return_sequences=False,
        input_shape=(LOOKBACK, 1)
    )
)
model.add(keras.layers.Dense(1))
model.compile(optimizer="adam", loss="mse")
model.summary()

print("[-INFO-] Training LSTM model...")
history = model.fit(
    X_train,
    y_train,
    epochs=EPOCHS,
    batch_size=BATCH_SIZE,
    validation_data=(X_test, y_test),
    verbose=1
)

#----------------PREDICTION----------------------
y_pred_scaled = model.predict(X_test)
y_test_inv = scaler.inverse_transform(y_test)
y_pred_inv = scaler.inverse_transform(y_pred_scaled)

#------------------METRICS----------------------
rmse = np.sqrt(mean_squared_error(y_test_inv, y_pred_inv))
mape = mean_absolute_percentage_error(y_test_inv, y_pred_inv)
r2 = r2_score(y_test_inv, y_pred_inv)

print("\n========== LSTM RESULTS ==========")
print(f"Coin:           {COIN_SYMBOL}")
print(f"Lookback:       {LOOKBACK} days")
print(f"Train/Test:     {int(TRAIN_RATIO*100)}% / {int((1-TRAIN_RATIO)*100)}%")
print(f"RMSE:           {rmse:.4f}")
print(f"MAPE:           {mape:.4f}")
print(f"R-squared (R²): {r2:.4f}")
print("==================================\n")

#--------------PLOT REAL vs PREDICTED-----------
plt.figure(figsize=(10, 5))
plt.plot(y_test_inv, label="Real price")
plt.plot(y_pred_inv, label="Predicted price")
plt.title(f"LSTM price prediction for {COIN_SYMBOL}")
plt.xlabel("Time step (test set)")
plt.ylabel("Price")
plt.legend()
plt.tight_layout()
plt.show()
