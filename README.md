# Field of Research Classification (FoRC)
<p align="center">
  <img src="https://github.com/ryabhmd/nfdi4ds-forc/assets/77779090/c9a3abec-0742-4c39-86da-5df76ce253a5" />

</p>

This is a repository for the dataset of the **Field of Research Classification (FoRC)** shared task, as part of the [NFDI for Data Science and Artificial Intelligence](https://www.nfdi4datascience.de/) project. Additionally, it is used for my master's thesis at Osnabrück University's Institute of Cognitive Science. 

## Overview

This repository is for constructing a dataset that can be used for the task of classifying scholarly papers into fields of research (FoR). The labels (i.e. FoR) are derived from the [Open Research Knowledge Graph (ORKG) research fields taxonomy](https://orkg.org/fields). The dataset is constructed based on two different sources, the ORKG, and [arXiv](https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download). Abstracts are added from [Crossref API](https://www.crossref.org/), [Semantic Scholar Academic Graph (S2AG) API](https://www.semanticscholar.org/product/api), and [OpenAlex](https://openalex.org/). 

## Pipeline

The dataset construction pipeline consists of:
1. Querying the ORKG rdfDump to get scholarly papers that contain the research field property (https://orkg.org/property/P30) and getting their metadata.
2. Obtaining abstracts from from Crossref API, S2AG API, and OpenAlex.
3. Sampling the arXiv snapshot with a random threshold (default is 50K) while keeping the original distribution.
4. Merging the two datasets and preprocessing.

## How to run

Before running the code, the following datasets need to be installed:

1. An arXiv snapshot: https://www.kaggle.com/datasets/Cornell-University/arxiv?resource=download. Note that the path in ```data_processing/process_arxiv_data.py``` needs to be modified if changed from the default.
2. The file ```lid.176.bin``` from the fastText language identification package: https://fasttext.cc/docs/en/language-identification.html. The file needs to be unzipped and the path in ```data_processing/data_cleaning_utils.py``` needs to be modified if changed from the default.

Navigate to the repository directory and run the following commands:

### Requirements

```commandline
pip install -r requirements.txt
```

### Dataset construction

```commandline
python data_processing/process_merged_data.py
```


This will create a dataset at ```data_processing/data/merged_data.csv```. 

### Contribution

This repository was developed by Raia Abu Ahmad (raia.abu_ahmad@dfki.de).

The initial basis for the data construction code was developed by the ORKG team. We used their code and developed it further. Their current version can be found at https://gitlab.com/TIBHannover/orkg/nlp/experiments/orkg-research-fields-classifier.
