import os
import pandas as pd

import torch
from transformers import AutoTokenizer, AutoModel


def get_scincl_embeddings(data_batch, tokenizer, model):
    """
    input: data_batch as a pd.DataFrame
    output: torch.Tensor containing the embeddings of the title and abtract of each row in the input
    """
    # # remove nan from abstracts
    data_batch['abstract'] = ["" if pd.isna(abstract) else abstract for abstract in data_batch['abstract']]

    # concatenate title and abstract with [SEP] token
    data_batch['title_abstract'] = [row['title'] + tokenizer.sep_token + (row['abstract'] or '')
                                    for index, row in data_batch.iterrows()]
    title_abstract_list = data_batch['title_abstract'].values.tolist()
    # preprocess the input
    inputs = tokenizer(title_abstract_list,
                       padding=True, truncation=True,
                       return_tensors="pt",
                       max_length=512)
    # inference
    result = model(**inputs)

    # take the first token ([CLS] token) in the batch as the embedding
    embeddings = result.last_hidden_state[:, 0, :]

    return embeddings


if __name__ == "__main__":
    # load the model and the tokenizer
    tokenizer = AutoTokenizer.from_pretrained('malteos/scincl')
    model = AutoModel.from_pretrained('malteos/scincl')

    # get the data
    merged_df = pd.read_csv('/netscratch/abu/forc_I_dataset.csv')
    baseline_data = merged_df[['title', 'abstract', 'label']]

    if not os.path.exists("/netscratch/abu/FoRC-embeddings/SciNCL/forc_I_embeddings_20230315"):
        os.makedirs("/netscratch/abu/FoRC-embeddings/SciNCL/forc_I_embeddings_20230315")

    for batch in range(1980):
        if batch == 1979:
            sample_data = baseline_data[batch * 30:]
        else:
            sample_data = baseline_data[batch * 30:(batch + 1) * 30]
        batch_embeddings = get_scincl_embeddings(sample_data, tokenizer, model)
        print(f'Got embeddings for batch {batch}')
        torch.save(batch_embeddings, f"/netscratch/abu/FoRC-embeddings/SciNCL/forc_I_embeddings_20230315/embeddings_{batch}.pt")
