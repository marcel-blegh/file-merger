import glob
import json
import pandas as pd


def get_pos(x: list):
    pos = [0]*len(x)
    pos[0] = 1
    pos[-1] = 2
    return pos


with open('./config.py', 'r') as f:
    config = json.loads(f.read())
pmids_all = pd.DataFrame()
for filename in glob.glob(f'{config["project_path"]}/data2024/pubmed/*'):
    pmids_sub = pd.read_csv(filename, dtype=object)
    pmids_sub.columns = ['pubmed_id']
    domain = (filename.split('\\')[-1].replace('.txt', '')).split('_')[0]
    pmids_sub['domain'] = domain
    query_type = (filename.split('\\')[-1].replace('.txt', '')).split('_')[-1]
    if query_type == domain:
        query_type = "full"
    pmids_sub[query_type] = True
    pmids_all = pd.concat([pmids_all, pmids_sub], axis=0)
print("regel 26", pmids_all.describe())
pmids_all.domain.unique()

methods = (pd.read_csv(f'{config["project_path"]}/methods.csv').map(lambda x: x.strip() if not pd.isna(x) else x))
abbreviations = (pd.read_csv(f'{config["project_path"]}/abbreviations.csv', sep=',')).map(lambda x: x.strip() if not pd.isna(x) else x)
clinical_domains = ['CLIN-DATA', 'CLIN-TRIAL', 'CLIN-GL', 'REV']
print(abbreviations.columns)
methods_list = list(abbreviations.short[abbreviations.long.isin(methods['method'].dropna())])
print(len(methods_list))

pmids_cancers = pmids_all.copy()[~pmids_all.domain.isin(clinical_domains + methods_list)]
pmids_rest = pmids_cancers.copy()[pmids_cancers.domain == 'REST']
pmids_cancers = pmids_cancers[pmids_cancers.domain != 'REST']
pmids_rest = pmids_rest[~pmids_rest.pubmed_id.isin(pmids_cancers.pubmed_id)]
pmids_cancers = pd.concat([pmids_cancers, pmids_rest], axis=0)

pmids_clinical = pmids_all.copy()[pmids_all['domain'].isin(clinical_domains)]
pmids_methods = pmids_all.copy()[pmids_all['domain'].isin(methods_list)]
pmids_methods = pmids_methods[pmids_methods['pubmed_id'].isin(pmids_cancers['pubmed_id'])]
# use one of the next two lines
# pmids_methods.drop_duplicates(['pubmed_id', 'domain', 'full', 'ti', 'tiab', 'ot', 'mesh'], keep='first', inplace=True)
pmids_methods.drop_duplicates(['pubmed_id', 'domain', 'full'], keep='first', inplace=True)
pmids_ge = pmids_cancers.copy()[pmids_cancers['domain'].isin(['HPB', 'Up_GI', 'Lo-GI'])]
pmids_ge = pmids_ge.drop_duplicates('pubmed_id', keep='first')
pmids_ge['domain'] = 'GE'
pmids_cancers = pd.concat([pmids_cancers, pmids_ge], axis=0)


pmids_cancers = pmids_cancers.merge(abbreviations[abbreviations['type'] == 'keyword'], how='inner', left_on='domain', right_on='short')
pmids_methods = pmids_methods.merge(abbreviations[abbreviations['type'] == 'method'], how='inner', left_on='domain', right_on='short')
print('finished loading pubmed data')

scopus_all = pd.DataFrame({
    'pubmed_id': pd.Series(dtype='str')
})
for filename in glob.glob(f'{config["project_path"]}/data2024/scopus/*.csv'):
    institute = filename.split('\\')[-1].split('.')[0].split('_')[0]       # filename = institute _ year
    chunks = []
    for chunk in pd.read_csv(filename, dtype=object, chunksize=2000):
        chunks.append(chunk)
    scopus = pd.concat(chunks, axis=0)
    scopus = scopus[scopus['pubmed_id'].isin(pmids_cancers['pubmed_id'])]
    scopus['institute'] = institute
    scopus_all = pd.concat([scopus_all, scopus], axis=0)
scopus_all.institute.unique()
print('finished loading scopus data')

scopus_ams = scopus_all.copy()[scopus_all['institute'].isin(['AUMC', 'NKIA'])]
scopus_ams = scopus_ams.drop_duplicates('pubmed_id', keep='first')
scopus_ams['institute'] = 'AMS'
scopus_all = pd.concat([scopus_all, scopus_ams], axis=0)
scopus_amsu = scopus_all.copy()[scopus_all['institute'].isin(['AMS', 'NKIA', 'UMCU'])]
scopus_amsu = scopus_amsu.drop_duplicates('pubmed_id', keep='first')
scopus_amsu['institute'] = 'AMSU'
scopus_all = pd.concat([scopus_all, scopus_amsu], axis=0)

assert len(scopus_all[scopus_all.duplicated(['institute', 'eid'])]) == 0, 'bad merge'
assert len(scopus_all[pd.isna(scopus_all['eid'])]) == 0, 'null eid'
assert len(scopus_all[scopus_all['pubmed_id'].isna()]) == 0, 'null pubmed_id'

scopus_all.dropna(subset=['pubmed_id'], inplace=True)
print(len(scopus_all[scopus_all.duplicated(['institute', 'pubmed_id'])].sort_values('pubmed_id', ascending=False)))
institutes = scopus_all.copy()[['institute', 'eid']]

publications = scopus_all.copy().drop_duplicates('pubmed_id', keep='first').drop(columns='institute')

institutes = institutes.merge(abbreviations[abbreviations['type'] == 'affiliation'], how='left', left_on='institute', right_on='short')
print(len(institutes[pd.isna(institutes["long"])]))
print(institutes[pd.isna(institutes["long"])]["institute"])
assert len(institutes[pd.isna(institutes["long"])]) == 0, 'bad merge'

publications['clinical'] = publications['pubmed_id'].isin(pmids_clinical[pmids_clinical['domain'] == 'CLIN-DATA']['pubmed_id'])
publications['trial'] = publications['pubmed_id'].isin(pmids_clinical[pmids_clinical['domain'] == 'CLIN-TRIAL']['pubmed_id'])
publications['guidelines'] = publications['pubmed_id'].isin(pmids_clinical[pmids_clinical['domain'] == 'CLIN-GL']['pubmed_id'])
publications['review'] = publications['pubmed_id'].isin(pmids_clinical[pmids_clinical['domain'] == 'REV']['pubmed_id'])
publications['doi'] = publications['doi'].apply(lambda x: x if pd.isna(x) else x.lower())

# a_positions = ['middle', 'first', 'last']
sub_affiliations = pd.read_csv(f'{config['project_path']}/sub_affiliations_selection.csv', dtype=object)
sub_affiliations = sub_affiliations[['institute', 'sub_afid']]

af_ams = sub_affiliations.copy()[sub_affiliations['institute'].isin(['AUMC', 'NKIA'])]
af_ams['institute'] = 'AMS'
af_amsu = sub_affiliations.copy()[sub_affiliations['institute'].isin(['AMS', 'NKIA', 'UMCU'])]
af_amsu['institute'] = 'AMSU'

sub_affiliations = pd.concat([sub_affiliations, af_ams, af_amsu], axis=0)
sub_affiliations['sub_afid'].apply(lambda x: str(x))

authors = scopus_all.copy()[['eid', 'author_afids']]
authors = authors[~pd.isna(authors['author_afids'])]
authors['author_afids'] = authors['author_afids'].apply(lambda x: x.split(';'))
# hier de lege velden weggooien
authors['position'] = authors['author_afids'].apply(get_pos)
authors = authors.explode(['author_afids', 'position'])
authors = authors[authors['author_afids'] != '']
authors['author_afids'] = authors['author_afids'].apply(lambda x: x.split('-'))
authors = authors.explode('author_afids')
authors = sub_affiliations.merge(authors, how='left', left_on='sub_afid', right_on='author_afids')
authors = authors.drop_duplicates(['eid', 'sub_afid', 'position']).drop(columns='author_afids')

authors = authors[~pd.isna(authors['eid'])]

aumc_positions = authors[authors['sub_afid'] == 'AUMC'].groupby(['eid']).sum()
aumc_positions['firstlast'] = aumc_positions['position'].apply(lambda x: x > 0)
publications = publications.merge(aumc_positions.drop(columns=['sub_afid', 'position']),
                                  how='left', on='eid')
publications.to_csv(f'{config["project_path"]}/tables2024/publications.csv',
                    index=False)
all_positions = authors.groupby(['eid', 'institute']).sum()
all_positions['firstlast'] = all_positions['position'].apply(lambda x: x > 0)
institutes = institutes.merge(all_positions.drop(columns=['sub_afid', 'position']),
                              how='left', on=['eid', 'institute'])
institutes.to_csv(f'{config["project_path"]}/tables2024/institutes.csv', index=False)

pmids_cancers = pmids_cancers[pmids_cancers.pubmed_id.isin(publications.pubmed_id)]
pmids_methods = pmids_methods[pmids_methods.pubmed_id.isin(publications.pubmed_id)]
pmids_cancers.to_csv(f'{config["project_path"]}/tables2024/keywords.csv', index=False)
pmids_methods.to_csv(f'{config["project_path"]}/tables2024/methods.csv', index=False)
