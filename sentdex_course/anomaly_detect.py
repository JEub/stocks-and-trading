import pandas as pd
import numpy as np

def deviations_from_mean(data, values, group_by=None, window=5, sigma=1.96):

    # Verify data is correct type
    assert isinstance(data, pd.DataFrame)

    if group_by is not None and group_by not in data.columns.tolist():
        raise KeyError('{} is not in the columns of the DataFrame object.'.format(group_by))

    if values not in data.columns.tolist():
        raise KeyError('{} is not in the columns of the DataFrame object.'.format(group_by))

    # initializing return data
    df = pd.DataFrame()

    if group_by is not None:
        for name in data[group_by].unique().tolist():
            temp = data.loc[data[group_by]==name,:].copy()
            temp['rolling_mean'] = temp[values].rolling(window, min_periods=1).mean()
            temp['resid'] = temp[values] - temp['rolling_mean']

            ## computing the rolling standard deviation on the residuals
            temp['std'] = temp['resid'].rolling(window, min_periods=1).std()
            temp['std'].fillna(temp['std'].mean(), inplace=True)

            ## defining lower and upper limits that would indicate an abnormality
            temp['lower'] = temp['rolling_mean']-(temp['std']*sigma)
            temp['upper'] = temp['rolling_mean']+(temp['std']*sigma)

            temp['anomaly'] = 0
            temp.loc[(temp[values]<temp['lower'])|(temp[values]>temp['upper']), 'anomaly'] = 1

            del temp['resid'], temp['lower'], temp['upper'], temp['rolling_mean'], temp['std']

            ## storing the results in the return data
            df = pd.concat(df,temp)
            del temp

    else:
        temp = data.copy()
        temp = data.loc[data[group_by]==name,:].copy()
        temp['rolling_mean'] = temp[values].rolling(window, min_periods=1).mean()
        temp['resid'] = temp[values] - temp['rolling_mean']

        ## computing the rolling standard deviation on the residuals
        temp['std'] = temp['resid'].rolling(window, min_periods=1).std()
        temp['std'].fillna(temp['std'].mean(), inplace=True)

        ## defining lower and upper limits that would indicate an abnormality
        temp['lower'] = temp['rolling_mean']-(temp['std']*sigma)
        temp['upper'] = temp['rolling_mean']+(temp['std']*sigma)

        temp['anomaly'] = 0
        temp.loc[(temp[values]<temp['lower'])|(temp[values]>temp['upper']), 'anomaly'] = 1

        del temp['resid'], temp['lower'], temp['upper'], temp['rolling_mean'], temp['std']

        ## storing the results in the return data
        df = pd.concat(df,temp)
        del temp

    return df
