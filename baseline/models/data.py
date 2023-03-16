import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer


class data_provider:
    def __init__(self, PATH="merged_data_fuzzywuzzy_dedup.csv",
                 features=['title', 'abstract', 'label']):
        self.data = pd.read_csv(PATH)
        print(self.data.head())
        self.features = features
        self.baseline_data = self.data[self.features]

    def convert_labels_to_ids(self):
        self.baseline_data['label_id'] = self.baseline_data['label'].factorize()[0]
        # id_to_label = dict(self.baseline_data[['label_id', 'label']].values)
        # label_id_df = self.baseline_data[['label', 'label_id']].drop_duplicates().sort_values('label_id')
        # label_to_id = dict(label_id_df.values)

    def preprocess_text(self):
        # remove NaN from abstracts
        self.baseline_data['abstract'] = ["" if pd.isna(abstract) else abstract
                                          for abstract in self.baseline_data['abstract']]

        # lowercase
        self.baseline_data['title'] = [title.lower() for title in self.baseline_data['title']]
        self.baseline_data['abstract'] = [abstract.lower() for abstract in self.baseline_data['abstract']]

    def get_features(self):
        tfidf = TfidfVectorizer(sublinear_tf=True,
                                min_df=5,
                                norm='l2',
                                encoding='latin-1',
                                ngram_range=(1, 2),
                                stop_words='english')
        self.preprocess_text()
        return tfidf.fit_transform(self.baseline_data.title + self.baseline_data.abstract).toarray()

    def get_labels(self):
        self.convert_labels_to_ids()
        return self.baseline_data.label_id
