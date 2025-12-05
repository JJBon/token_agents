import json
import os
from typing import Any, Dict

import joblib
import pandas as pd
from sklearn.metrics import make_scorer, f1_score, precision_score, recall_score
from sklearn.model_selection import GridSearchCV, StratifiedKFold, train_test_split
from sklearn.tree import DecisionTreeClassifier, export_text

from servers.src.logger import logger
from servers.src.variables import DATAFRAME_ASSISTANT_SAMPLE_CSV_PATH, DECISION_TREE_DIR


def build_decision_tree_classifier(target_variable: str, average: str = "weighted") -> Dict[str, Any]:
    """
    Train a decision tree model and return {"model_uri": "...", "metrics": {...}}.    
    """
    df = pd.read_csv(DATAFRAME_ASSISTANT_SAMPLE_CSV_PATH)

    COL_MAPPER = dict()
    CLASS_TO_LABEL = dict()

    categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()
    
    target_label_mapping = {e: i for i, e in enumerate(set(df[target_variable]))}
    df[target_variable] = df[target_variable].apply(lambda x: target_label_mapping[x])

    target_var_value_set = set(target_label_mapping.keys())

    if target_variable in categorical_cols:
        categorical_cols.remove(target_variable)

    for col in categorical_cols:
        col_unique_vals = set(df[col])

        if col_unique_vals.issubset(target_var_value_set):
            df[col] = df[col].apply(lambda x: target_label_mapping[x])
            COL_MAPPER[col] = target_label_mapping
        else:
            ordinal_encoder = {e: i for i, e in enumerate(df[col].unique())}
            df[col] = df[col].apply(lambda x: ordinal_encoder[x])
            COL_MAPPER[col] = ordinal_encoder
    
    COL_MAPPER[target_variable] = target_label_mapping
    CLASS_TO_LABEL = {v: k for k, v in target_label_mapping.items()}

    data_X = df.drop(columns=[target_variable])
    data_y = df[target_variable]

    X_train, X_test, y_train, y_test = train_test_split(data_X, data_y, test_size=0.2, random_state=42, stratify=data_y)
    
    logger.info(f"#Train examples = {X_train.shape[0]}")
    logger.info(f"#Test examples = {X_test.shape[0]}")

    clf = DecisionTreeClassifier(random_state=42, class_weight="balanced")
    param_grid = {
        "max_depth": [3, 5, 7, 10],
        "min_samples_split": [2, 4, 7],
        "min_samples_leaf": [1, 2, 4]
    }
    cv_strategy = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    f1_scorer = make_scorer(f1_score, average='macro') if len(CLASS_TO_LABEL) > 2 else make_scorer(f1_score, average='binary')
    grid_search = GridSearchCV(
        estimator=clf,
        param_grid=param_grid,
        cv=cv_strategy,        # stratified folds
        scoring=f1_scorer,
        n_jobs=-1,
        verbose=1
    )

    grid_search.fit(X_train, y_train)

    logger.info(f"Best Parameters: {grid_search.best_params_}", )
    logger.info(f"Best CV Macro F1: {grid_search.best_score_}")
    logger.info("----------\n")

    clf_best = grid_search.best_estimator_

    y_train_pred = clf_best.predict(X_train)
    y_test_pred = clf_best.predict(X_test)

    train_data_metrics = {
        f"Recall ({average})": recall_score(y_train, y_train_pred, average=average, zero_division=0),
        f"Precision ({average})": precision_score(y_train, y_train_pred, average=average, zero_division=0),
        f"F1-score ({average})": f1_score(y_train, y_train_pred, average=average, zero_division=0)
    }
    test_data_metrics = {
        f"Recall ({average})": recall_score(y_test, y_test_pred, average=average, zero_division=0),
        f"Precision ({average})": precision_score(y_test, y_test_pred, average=average, zero_division=0),
        f"F1-score ({average})": f1_score(y_test, y_test_pred, average=average, zero_division=0)
    }


    logger.info('---- Train Results ----')
    logger.info(f"Recall ({average}) = {train_data_metrics[f'Recall ({average})']}")
    logger.info(f"Precision ({average}) = {train_data_metrics[f'Precision ({average})']}")
    logger.info(f"F1-score ({average}) = {train_data_metrics[f'F1-score ({average})']}")

    logger.info("----------\n")
    
    logger.info('---- Test Results ----')
    logger.info(f"Recall ({average}) = {test_data_metrics[f'Recall ({average})']}")
    logger.info(f"Precision ({average}) = {test_data_metrics[f'Precision ({average})']}")
    logger.info(f"F1-score ({average}) = {test_data_metrics[f'F1-score ({average})']}")

    os.makedirs(DECISION_TREE_DIR, exist_ok=True)
    joblib.dump(clf_best, DECISION_TREE_DIR / "model.joblib")

    with open(DECISION_TREE_DIR / "ordinal_encoding.json", "w") as f:
        json.dump(COL_MAPPER, f, indent=4)
    
    with open(DECISION_TREE_DIR / "class_encodings.json", "w") as f:
        json.dump(CLASS_TO_LABEL, f, indent=4)
    
    df.to_csv(DECISION_TREE_DIR / "processed_data.csv", encoding='utf-8', index=False)

    return {
        "model_uri": (DECISION_TREE_DIR / "model.joblib").as_posix(), 
        "metrics": {"train": train_data_metrics, "test": test_data_metrics}
    }


def load_trained_model() -> Dict[str, Any]:
    """Helper function for loading the trained decision tree model, related encodings, and processed dataframe."""
    model_uri = DECISION_TREE_DIR / "model.joblib"
    clf_model = joblib.load(model_uri)

    with open(DECISION_TREE_DIR / "ordinal_encoding.json", "r") as f:
        col_ordinal_encoder = json.load(f)
    
    with open(DECISION_TREE_DIR / "class_encodings.json", "r") as f:
        class_encodings = json.load(f)
    
    df = pd.read_csv(DECISION_TREE_DIR / "processed_data.csv")
    
    return {
        "decision_tree_model": clf_model,
        "column_ordinal_encodings": col_ordinal_encoder,
        "class_to_label": class_encodings,
        "df": df
    }


def compute_accuracy_metrics(target_variable: str, average: str) -> Dict[str, dict]:
    """
    Evaluate a trained decision tree model on train/test splits and log metrics.
    
    Parameters
    ----------
    target_variable : str
        The target column name in the dataset.
    average : str
        Averaging method for multi-class metrics (e.g., "micro", "macro", "weighted").
    """
    model = load_trained_model()

    clf = model['decision_tree_model']
    df = model['df']

    data_X = df.drop(columns=[target_variable])
    data_y = df[target_variable]

    X_train, X_test, y_train, y_test = train_test_split(data_X, data_y, test_size=0.2, random_state=42, stratify=data_y)

    y_train_pred = clf.predict(X_train)
    y_test_pred = clf.predict(X_test)

    train_data_metrics = {
        f"Recall ({average})": recall_score(y_train, y_train_pred, average=average, zero_division=0),
        f"Precision ({average})": precision_score(y_train, y_train_pred, average=average, zero_division=0),
        f"F1-score ({average})": f1_score(y_train, y_train_pred, average=average, zero_division=0)
    }
    test_data_metrics = {
        f"Recall ({average})": recall_score(y_test, y_test_pred, average=average, zero_division=0),
        f"Precision ({average})": precision_score(y_test, y_test_pred, average=average, zero_division=0),
        f"F1-score ({average})": f1_score(y_test, y_test_pred, average=average, zero_division=0)
    }

    logger.info('---- Train Results ----')
    logger.info(f"Recall ({average}) = {train_data_metrics[f'Recall ({average})']}")
    logger.info(f"Precision ({average}) = {train_data_metrics[f'Precision ({average})']}")
    logger.info(f"F1-score ({average}) = {train_data_metrics[f'F1-score ({average})']}")

    logger.info("----------\n")
    
    logger.info('---- Test Results ----')
    logger.info(f"Recall ({average}) = {test_data_metrics[f'Recall ({average})']}")
    logger.info(f"Precision ({average}) = {test_data_metrics[f'Precision ({average})']}")
    logger.info(f"F1-score ({average}) = {test_data_metrics[f'F1-score ({average})']}")

    return {'train': train_data_metrics, 'test': test_data_metrics}


def model_inference(feature_values: Dict[str, list]) -> Dict[str, list]:
    """Load model and return {"prediction": ...}."""
    for k, v in feature_values.items():
        if not isinstance(v, list):
            feature_values[k] = [v]

    model = load_trained_model()
    clf = model['decision_tree_model']
    class_to_label = model['class_to_label']
    
    X_test = pd.DataFrame.from_dict(feature_values)
    for col, encoding in list(model['column_ordinal_encodings'].items())[:-1]:
        X_test[col] = X_test[col].apply(lambda x: encoding[x])

    predicted_classes = clf.predict(X_test)
    prediction = [class_to_label[str(item)] for item in predicted_classes]

    return {"prediction": prediction}


def export_decision_tree_to_text() -> str:
    """Return a multiline string containing the decision rules of the tree, formatted as nested if/else statements."""
    model = load_trained_model()

    clf = model['decision_tree_model']
    feature_names = clf.feature_names_in_
    class_names = list(model['class_to_label'].values())

    tree_rules = export_text(clf, feature_names=feature_names, class_names=class_names)
    return tree_rules
