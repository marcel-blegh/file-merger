import glob
import json
import pandas as pd


def get_pos(x: list):
    pos = [0]*len(x)
    pos[0] = 1
    pos[-1] = 2
    return pos


with open('./config.py') as f:
    config = json.loads(f.read())
# fetch all keywords from scopus,
# those should be in the data/obsgyn/scopus/keywords/ directory on your local machine
scopus_all = pd.DataFrame({
    'pubmed_id': pd.Series(dtype='object'),
    'doi': pd.Series(dtype='object'),
    'domain': pd.Series(dtype='object')
})
for filename in glob.glob(f'{config["project_path"]}/data2024/scopus/Keywords/*.csv'):
    print(filename)
    scopus = pd.read_csv(filename, dtype=object)
    scopus_keyword = pd.DataFrame({
        'pubmed_id': pd.Series(dtype='str'),
        'doi': pd.Series(dtype='str')
    })
    scopus_keyword[['pubmed_id', 'doi']] = scopus[['pubmed_id', 'doi']]
    scopus_keyword["domain"] = filename.split("\\")[-1].split(".")[0]
    scopus_all = pd.concat((scopus_all, scopus_keyword), ignore_index=True, axis="rows")

methods = (pd.read_csv(f'{config["project_path"]}/methods.csv').map(
    lambda x: x.strip() if not pd.isna(x) else x))
print(len(methods))
abbreviations = (pd.read_csv(f'{config["project_path"]}/abbreviations.csv', sep=';').map(
    lambda x: x.strip() if not pd.isna(x) else x))
clinical_domains = ['CLIN-DATA', 'CLIN-TRIAL', 'CLIN-GL', 'REV']
methods_list = list(abbreviations.short[abbreviations.long.isin(methods['method'].dropna())])

scopus_all_kw = scopus_all.copy()[~scopus_all.domain.isin(methods_list + clinical_domains)]
scopus_all_rest = scopus_all_kw.copy()[scopus_all_kw.domain == 'REST']
scopus_all_kw = scopus_all_kw[scopus_all_kw.domain != 'REST']
scopus_all_rest = scopus_all_rest[~scopus_all_rest.doi.isin(scopus_all_kw.doi)]
scopus_all_kw = pd.concat([scopus_all_kw, scopus_all_rest], axis=0)
print("scopus all by keyword", len(scopus_all_kw))

scopus_all_clinical = scopus_all.copy()[scopus_all.domain.isin(clinical_domains)]
scopus_all_methods = scopus_all.copy()[scopus_all.domain.isin(methods_list)]
scopus_all_methods = scopus_all_methods[scopus_all_methods.doi.isin(scopus_all_kw['doi'])]
scopus_all_methods.drop_duplicates(['pubmed_id', 'domain', 'doi'], keep='first', inplace=True)
print("scopus_all_methods", len(scopus_all_methods))

scopus_all_kw = scopus_all_kw.merge(abbreviations[abbreviations['type'] == 'keyword'],
                                    how='inner', left_on='domain', right_on='short')
scopus_all_methods = scopus_all_methods.merge(abbreviations[abbreviations['type'] == 'method'],
                                              how='inner', left_on='domain', right_on='short')
print('Finished loading the scopus keyword data \n\n')
# fetch all affiliations from scopus,
# those should be in the data2024/scopus directory on teams

affiliations_all = pd.DataFrame()
for filename in glob.glob(f'{config["project_path"]}/data2024/scopus/Affiliations/*.csv'):
    # remove filepath and extension to get the institute name
    institute = filename.split('\\')[-1].split('.')[0]
    chunks = []
    for chunk in pd.read_csv(filename, dtype=object, chunksize=2000):
        chunks.append(chunk)
    affiliations = pd.concat(chunks, axis=0, ignore_index=True)
    affiliations = affiliations[affiliations['doi'].isin(scopus_all_kw['doi'])]
    affiliations["institute"] = institute
    affiliations_all = pd.concat((affiliations_all, affiliations), ignore_index=True, axis="rows")
print('Finished loading the scopus affiliation data \n\n')
print(len(affiliations_all))

affiliations_all.dropna(subset=['doi'], inplace=True)
institutes = affiliations_all.copy()[['institute', 'eid']]
institutes.merge(abbreviations[abbreviations['type'] == 'affiliation'], how='left', left_on='institute', right_on='short')
publications = affiliations_all.copy().drop_duplicates('doi', keep='first').drop(columns='institute')

publications['clinical'] = publications['doi'].isin(scopus_all_clinical[scopus_all_clinical['domain'] == 'CLIN-DATA']['doi'])
publications['trial'] = publications['doi'].isin(scopus_all_clinical[scopus_all_clinical['domain'] == 'CLIN-TRIAL']['doi'])
publications['guidelines'] = publications['doi'].isin(scopus_all_clinical[scopus_all_clinical['domain'] == 'CLIN-GL']['doi'])
publications['review'] = publications['doi'].isin(scopus_all_clinical[scopus_all_clinical['domain'] == 'REV']['doi'])
publications['doi'] = publications['doi'].apply(lambda x: x if pd.isna(x) else x.lower())

publications.to_csv(f'{config["project_path"]}/tables2024/scopus_publications.csv', index=False)

sub_affiliations = pd.read_csv(f'{config['project_path']}/sub_affiliations_selection.csv', dtype=object)
sub_affiliations = sub_affiliations[['institute', 'sub_afid']]
sub_affiliations['sub_afid'].apply(lambda x: str(x))

authors = affiliations_all.copy()[['eid', 'author_afids']]
authors = authors[~pd.isna(authors['author_afids'])]
authors['author_afids'] = authors['author_afids'].apply(lambda x: x.split(';'))
authors['position'] = authors['author_afids'].apply(get_pos)
authors = authors.explode(['author_afids', 'position'])
authors = authors[authors['author_afids'] != '']
authors['author_afids'] = authors['author_afids'].apply(lambda x: x.split('-'))
authors = authors.explode('author_afids')
authors = sub_affiliations.merge(authors, how='left', left_on='sub_afid', right_on='author_afids')
authors = authors.drop_duplicates(['eid', 'sub_afid', 'position'], keep='first').drop(columns='author_afids')

authors = authors[~pd.isna(authors['eid'])]

all_positions = authors.groupby(['eid', 'institute']).sum()
all_positions['firstlast'] = all_positions['position'].apply(lambda x: x > 0)
institutes = institutes.merge(all_positions.drop(columns=['sub_afid', 'position']),
                              how='left', on=['eid', 'institute'])

scopus_all_kw = scopus_all_kw[scopus_all_kw.doi.isin(publications.doi)]
scopus_all_kw.to_csv(f'{config["project_path"]}/tables2024/scopus_keywords.csv', index=False)
scopus_all_methods = scopus_all_methods[scopus_all_methods.doi.isin(publications.doi)]
scopus_all_methods.to_csv(f'{config["project_path"]}/tables2024/scopus_methods.csv', index=False)

# 10.1016/j.aquaculture.2024.741332