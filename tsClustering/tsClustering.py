# modules
import pandas as pd
import matplotlib.pyplot as plt

# path to the datasets
dfPath = '..//crowdRegs2.pcl'
regsPath = '..//crowdRegs.csv'

def loadData():
    df = pd.read_pickle(dfPath)
    regsDf = pd.read_csv(regsPath,names=['id', 'regId'])
    df.columns = regsDf.regId.values.astype('str')
    return df


if __name__=='__main__':
    df = loadData()
    print df.head()
    #df.plot()
    #plt.show()