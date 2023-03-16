from data import data_provider
import pandas as pd

from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import LinearSVC
from sklearn.model_selection import cross_val_score

import seaborn as sns


class baseline_models:
    def __init__(self, random_state=42):
        self.data = data_provider()
        self.features = self.data.get_features()
        self.labels = self.data.get_labels()
        self.random_state = random_state

    def run_models(self):
        models = [
            RandomForestClassifier(n_estimators=200, max_depth=3, random_state=self.random_state),
            LinearSVC(),
            MultinomialNB(),
            LogisticRegression(random_state=self.random_state),
        ]
        CV = 5
        cv_df = pd.DataFrame(index=range(CV * len(models)))
        entries = []
        for model in models:
            model_name = model.__class__.__name__
            accuracies = cross_val_score(model, self.features, self.labels, scoring='accuracy', cv=CV)
            for fold_idx, accuracy in enumerate(accuracies):
                entries.append((model_name, fold_idx, accuracy))
        cv_df = pd.DataFrame(entries, columns=['model_name', 'fold_idx', 'accuracy'])

        sns.boxplot(x='model_name', y='accuracy', data=cv_df)
        sns.stripplot(x='model_name', y='accuracy', data=cv_df,
                      size=8, jitter=True, edgecolor="gray", linewidth=2)
        plt.show()

        print(cv_df.groupby('model_name').accuracy.mean())


if __name__ == '__main__':
    baseline = baseline_models()
    baseline.run_models()