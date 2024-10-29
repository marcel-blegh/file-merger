import json
import pandas as pd

with open('./config.py', 'r') as f:
    config = json.loads(f.read())
with open(f'{config["project_path"]}/tables2024/publications.csv', encoding='utf8') as f:
    pubmed_publications = pd.read_csv(f)
with open(f'{config["project_path"]}/tables2024/scopus_publications.csv', encoding='utf8') as f:
    scopus_publications = pd.read_csv(f)
scopus_publications['scopus'] = 1
pubmed_publications['pubmed'] = 1

pubmed_publications_ids = pubmed_publications[['pubmed_id', 'doi', 'pubmed', 'clinical', 'trial', 'guidelines', 'review']]
scopus_publications_ids = scopus_publications[['pubmed_id', 'doi', 'scopus', 'clinical', 'trial', 'guidelines', 'review']]

all_publication_ids = pd.concat([pubmed_publications_ids, scopus_publications_ids])
all_publication_ids = all_publication_ids.groupby(['pubmed_id', 'doi', 'clinical', 'trial', 'guidelines', 'review']).any()
all_publication_ids = all_publication_ids.reset_index()
all_publication_ids.drop_duplicates(['pubmed_id', 'doi'], keep='first', inplace=True)

all_publications_full = pd.concat([scopus_publications, pubmed_publications])
all_publications_full.drop(['scopus', 'pubmed', 'clinical', 'trial', 'guidelines', 'review'], axis=1, inplace=True)
all_publications_full.drop_duplicates(keep='first', inplace=True)

# # all_publications_full.drop(['eid'], axis=1, inplace=True)
#
# merged_publications = all_publication_ids.merge(all_publications_full, how='left', on=['pubmed_id', 'doi'])
# merged_publications.to_csv(f'{config["project_path"]}/tables2024/merged_publications.csv', index=False)

# vorige versie gooide alle pubmed_ids zonder doi weg, en vice versa
all_publications_pubmed = all_publication_ids[~pd.isna(all_publication_ids['pubmed_id'])]
all_publications_pubmed_merged = all_publications_pubmed.merge(all_publications_full, how='left', on='pubmed_id')
all_publications_scopus = all_publication_ids[pd.isna(all_publication_ids['pubmed_id'])]
all_publications_scopus_merged = all_publications_scopus.merge(all_publications_full, how='left', on='doi')
merged_publications = pd.concat([all_publications_pubmed_merged, all_publications_scopus], )
merged_publications.drop_duplicates(keep='first', inplace=True)
print(len(merged_publications))

merged_publications.to_csv(f'{config["project_path"]}/tables2024/merged_publications2.csv', index=True)

with open(f'{config["project_path"]}/tables2024/scopus_keywords.csv', encoding='utf8') as f:
    scopus_keywords = pd.read_csv(f)
with open(f'{config["project_path"]}/tables2024/keywords.csv', encoding='utf8') as f:
    pubmed_keywords = pd.read_csv(f)
scopus_keywords['scopus'] = 1
pubmed_keywords['pubmed'] = 1

all_keywords = pd.concat([scopus_keywords, pubmed_keywords], axis='rows')
all_keywords = all_keywords.groupby(['doi', 'domain', 'long', 'short', 'type', 'pubmed_id'])[['pubmed', 'scopus']].sum()
all_keywords = all_keywords.reset_index()
all_keywords.to_csv(f'{config["project_path"]}/tables2024/all_keywords.csv', index=True)



