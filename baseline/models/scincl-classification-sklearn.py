import torch
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
from scipy.sparse import hstack
from scipy import sparse

# Set the path to the directory where the embeddings are saved
embedding_dir = "/netscratch/abu/FoRC-embeddings/SciNCL/forc_I_embeddings_20230315/"

# Get features (embeddings)
embeddings = []
for i in range(1980):
    embedding_file = embedding_dir + f"embeddings_{i}.pt"
    embedding = torch.load(embedding_file)
    embeddings.append(embedding)

embeddings_tensor = torch.cat(embeddings, dim=0)

embeddings = []

embeddings_list = embeddings_tensor.tolist()

# transform to sparse
embeddings_sparse = sparse.csr_matrix(embeddings_list)

X = hstack([embeddings_sparse])
X_arr = X.toarray()

# Get labels 

y = torch.load("/netscratch/abu/forc-baseline-experiments/labels_forc_I_dataset.pt")

# Split data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X_arr, y, test_size = 0.2, random_state = 0)
print('X_train shape: ', X_train.shape)
print('X_test shape: ', X_test.shape)
print('y_train shape: ', y_train.shape)
print('y_test shape: ', y_test.shape)

# Train model
logreg = LogisticRegression(solver='lbfgs', multi_class='multinomial', random_state=0)
logreg.fit(X_train, y_train)

# Evaluation
y_pred_test = logreg.predict(X_test)
print('Model accuracy score: {0:0.4f}'. format(accuracy_score(y_test, y_pred_test)))
print('Model precision score: {0:0.4f}'. format(precision_score(y_test, y_pred_test, average='weighted')))
print('Model recall score: {0:0.4f}'. format(recall_score(y_test, y_pred_test, average='weighted')))
print('Model f1 score: {0:0.4f}'. format(f1_score(y_test, y_pred_test, average='weighted')))

# Classification Report
class_report = classification_report(y_test, y_pred_test)
print('Classification Report:\n', class_report)
