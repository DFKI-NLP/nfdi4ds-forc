# Field of Research Classification (FoRC)

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
