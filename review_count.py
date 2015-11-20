import pandas as pd


def loadJsonData(filename):
    with open(filename, 'rb') as f:
        data = f.readlines()

    # remove the trailing "\n" from each line
    data = map(lambda x: x.rstrip(), data)

    # each element of 'data' is an individual JSON object.
    # i want to convert it into an *array* of JSON objects
    # which, in and of itself, is one large JSON objectfr
    # basically... add square brackets to the beginning
    # and end, and have all the individual business JSON objects
    # separated by a comma
    data_json_str = "[" + ','.join(data) + "]"

    # now, load it into pandas dataframe and return it
    return pd.read_json(data_json_str)


''' 
df = dataframe to split,
target_column = the column containing the values to split

returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
The values in the other columns are duplicated across the newly divided rows.
'''
def splitDataFrameList(df, target_column):
    
    def splitListToRows(row, row_accumulator, target_column):
        split_row = row[target_column]
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)
    
    new_rows = []
    df.apply(splitListToRows,axis=1, args = (new_rows,target_column))
    new_df = pd.DataFrame(new_rows)
    return new_df


def getNumReviewsGroupedByUserAndCatergory():
    business = loadJsonData('yelp_academic_dataset_business.json')
    business = splitDataFrameList(business, 'categories')
    # filter business only for categories that we are working on
    user = loadJsonData('yelp_academic_dataset_user.json')
    review = loadJsonData('yelp_academic_dataset_review.json')

    df = users[['user_id']].merge(review[['user_id', 'business_id']], on='user_id').merge(business[['business_id', 'categories']], on='business_id')
    df_grpd = df.groupby(['user_id', 'categories'])

    columns = ['user_id', 'category', 'review_count']
    num_reviews = pd.DataFrame(columns=columns)

    i = 0
    for key,value in df_grp:
        num_reviews.loc[i] = [key[0], key[1], value['user_id'].count()]
        i += 1

    return num_reviews