"""Train baseline ML models on the gold feature table.

Two targets, trained per symbol (price dynamics differ enough across AAPL/NVDA/TSLA/AMD
that pooling them into one model tends to just learn "NVDA is more volatile" noise):
  - Classification: will tomorrow's close be up or down (target_up_next)
  - Regression:     what is tomorrow's return (target_return_next)

Time-based split, NOT random shuffling: the last `test_size_pct` of each symbol's
timeline is held out. Shuffling a time series before splitting leaks future
information into training via neighboring rows and will make your backtest look far
better than any real trading would -- this is one of the most common mistakes in
retail quant projects.

Tracks every run with MLflow (local file-based store, no server needed) so you can
compare model versions the same way you'd use MLflow on Databricks.
"""
import argparse
import os
from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor, RandomForestClassifier
from sklearn.metrics import accuracy_score, mean_absolute_error, roc_auc_score

from src.ml.feature_engineering import FEATURE_COLUMNS
from src.utils.config import load_config, resolve_path
from src.utils.spark_session import get_spark

MODEL_DIR_DEFAULT = "./data/models"


def time_based_split(pdf: pd.DataFrame, test_size_pct: float):
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)
    split_idx = int(len(pdf) * (1 - test_size_pct))
    return pdf.iloc[:split_idx], pdf.iloc[split_idx:]


def train_symbol(pdf: pd.DataFrame, symbol: str, cfg: dict, model_dir: str):
    pdf = pdf.dropna(subset=FEATURE_COLUMNS + ["target_up_next", "target_return_next"])
    if len(pdf) < 60:
        print(f"[train:{symbol}] not enough clean rows ({len(pdf)}) after dropping NaNs, skipping")
        return None

    train_df, test_df = time_based_split(pdf, cfg["ml"]["test_size_pct"])
    X_train, X_test = train_df[FEATURE_COLUMNS], test_df[FEATURE_COLUMNS]
    y_train_cls, y_test_cls = train_df["target_up_next"], test_df["target_up_next"]
    y_train_reg, y_test_reg = train_df["target_return_next"], test_df["target_return_next"]

    with mlflow.start_run(run_name=f"{symbol}_baseline"):
        mlflow.log_param("symbol", symbol)
        mlflow.log_param("n_train", len(train_df))
        mlflow.log_param("n_test", len(test_df))
        mlflow.log_param("test_size_pct", cfg["ml"]["test_size_pct"])

        clf = RandomForestClassifier(
            n_estimators=300, max_depth=6, min_samples_leaf=20,
            random_state=cfg["ml"]["random_state"], n_jobs=-1,
        )
        clf.fit(X_train, y_train_cls)
        proba_test = clf.predict_proba(X_test)[:, 1]
        pred_test = (proba_test > 0.5).astype(int)
        acc = accuracy_score(y_test_cls, pred_test)
        try:
            auc = roc_auc_score(y_test_cls, proba_test)
        except ValueError:
            auc = float("nan")  # only one class present in a short test window

        reg = GradientBoostingRegressor(
            n_estimators=200, max_depth=3, learning_rate=0.05,
            random_state=cfg["ml"]["random_state"],
        )
        reg.fit(X_train, y_train_reg)
        pred_reg = reg.predict(X_test)
        mae = mean_absolute_error(y_test_reg, pred_reg)
        naive_mae = mean_absolute_error(y_test_reg, np.zeros_like(y_test_reg))  # "predict no change" baseline

        mlflow.log_metric("test_accuracy", acc)
        mlflow.log_metric("test_auc", auc)
        mlflow.log_metric("test_mae", mae)
        mlflow.log_metric("naive_mae_predict_zero", naive_mae)

        print(
            f"[train:{symbol}] n_train={len(train_df)} n_test={len(test_df)} "
            f"| direction acc={acc:.3f} auc={auc:.3f} "
            f"| return MAE={mae:.5f} (naive={naive_mae:.5f})"
        )

        os.makedirs(model_dir, exist_ok=True)
        clf_path = os.path.join(model_dir, f"{symbol}_direction_clf.joblib")
        reg_path = os.path.join(model_dir, f"{symbol}_return_reg.joblib")
        joblib.dump(clf, clf_path)
        joblib.dump(reg, reg_path)
        mlflow.log_artifact(clf_path)
        mlflow.log_artifact(reg_path)

    test_df = test_df.copy()
    test_df["pred_proba_up"] = proba_test
    test_df["pred_return"] = pred_reg
    return {
        "symbol": symbol,
        "accuracy": acc,
        "auc": auc,
        "mae": mae,
        "naive_mae": naive_mae,
        "test_predictions": test_df,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", default=MODEL_DIR_DEFAULT)
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark(cfg)

    mlflow.set_tracking_uri("sqlite:///mlflow.db")
    mlflow.set_experiment("stock-direction-and-return-prediction")

    gold_root = resolve_path(cfg, "delta.gold_path")
    gold = spark.read.format("delta").load(f"{gold_root}/features")
    pdf = gold.toPandas()

    results = []
    all_predictions = []
    for symbol in cfg["symbols"]:
        sym_pdf = pdf[pdf["symbol"] == symbol]
        result = train_symbol(sym_pdf, symbol, cfg, args.model_dir)
        if result:
            results.append({k: v for k, v in result.items() if k != "test_predictions"})
            all_predictions.append(result["test_predictions"])

    print("\n=== Summary across symbols ===")
    for r in results:
        print(f"  {r['symbol']}: acc={r['accuracy']:.3f} auc={r['auc']:.3f} mae={r['mae']:.5f}")

    if all_predictions:
        preds_pdf = pd.concat(all_predictions, ignore_index=True)
        preds_path = f"{gold_root}/model_predictions"
        spark.createDataFrame(preds_pdf).write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(preds_path)
        preds_uri = Path(preds_path).resolve().as_uri()
        spark.sql(f"CREATE TABLE IF NOT EXISTS gold.model_predictions USING DELTA LOCATION '{preds_uri}'")

    spark.stop()


if __name__ == "__main__":
    main()
