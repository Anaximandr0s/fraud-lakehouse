# 0008 — Fraud model: Spark MLlib, threshold on validation, reproducible metrics

**Context.** Step 8 must train a model on `gold.ml_features` and report PR-AUC, precision and recall on the test
split. Frauds are 0.39 % of the test rows, so accuracy is meaningless, and the threshold behind precision and
recall is itself a choice that can leak the test set.

**Decision.** Spark MLlib on the cluster, not scikit-learn: training runs on the executors, while
scikit-learn would pull 1.3 M rows into a 512m driver. GBT, compared with LogisticRegression and with a random
model (PR-AUC = fraud rate). Both use the same features and class weights (each class weighs half). Each model's threshold is
the best F1, in 0.01 steps, on the latest 20 % of train (an exact time cutoff), never on test. PR-AUC uses the full
curve (`numBins=0`). Seed 42; the logic lives in `src/fraud_lakehouse/model.py`, tested on a local SparkSession.

**Consequences.** Three runs on the same data gave identical metrics. `results.md` records the Iceberg snapshot and
split sizes it was computed on. The threshold grid stops at 0.99; class weights push the best thresholds to
0.94–0.95 here. The notebook is committed without outputs (nbstripout); results live in the README.
