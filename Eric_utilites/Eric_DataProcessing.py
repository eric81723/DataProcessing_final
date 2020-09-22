import pandas as pd
import os

def f_PathFileName(PathName, FileName):
    '''
    Objective:
        產生跨平台的「路徑+檔案」的字串，以利檔案輸出時使用。若路徑不存在，自動在相對位置創建資料夾。
    Input:
        PathName: 路徑名稱
        FileName: 檔案名稱
    Output:
        跨平台的「路徑+檔案」的字串
    '''
    import os
    if not os.path.exists(PathName):
        os.makedirs(PathName)
    return os.path.join(PathName, FileName)

def f_CheckCSV(PathName, Sep):

    Data = list([])
    error = list([])

    with  open(PathName,newline='') as f:
        k=0
        for i in f:
            temp = i.split(Sep)
            temp = [j.strip() for j in temp]
            rows = len(temp)
            
            if k == 0:
                rows_header = rows
                print('Data is expected to have {} columns'.format(rows_header))

            if rows != rows_header:
                error.append((k,temp))
                print('Index {} has wrong length'.format(k))
                print('Data Information : {}'.format(temp))
                continue
            
            Data.append(temp)
            k = k + 1 
            
    
    # c = len(lines[0])
    # r = len(lines)
    # print('資料欄位數量： {:>7} \n資料列數： {:>10}'.format(c, r))
    # print('確認每一行欄位相同.....')
    # error = list([])
    # path = f_PathFileName(Ouptput,'error.csv')
    # with open(path, 'w') as f:
    #     writer = csv.writer(f, dialect=my_dialect)
    #     for i in range(r):
    #         line = lines[i]
    #         if len(line) != c:
    #             print('Index {} has different length {}'.format(i, len(line)))
    #             writer.writerow(line)
    #             error.append(line)
    print('The data has {} rows, and {} error rows.'.format(len(Data),len((error))))

    return Data, error

def f_Outputdtype(df,filename,path):

    import csv
    path = f_PathFileName(path,filename)
    dt = df.dtypes
    dt.to_csv(path)
 
def f_ReadDtypefile(filename,path):
    path = f_PathFileName(path,filename)
    with open(path) as f:
        mapping = dict()
        for i in f:
            temp = i.strip().split(',')
            key = temp[0].strip()
            value = temp[1].strip()
            mapping.update({key:value})
    
    return mapping

def f_ChangeDtype(df,mapping):
    import pandas as pd
    errors = [ i  for i in mapping.keys() if i not in df.columns]
    for i in errors:
        del mapping[i]

    for i in mapping.keys():
        if mapping[i] in ['int','float','int32','int64']:
            df[i] = df[i].apply(pd.to_numeric,errors='coerce')
        elif mapping[i] in ['datetime']:
            df[i] = df[i].apply(pd.to_datetime, errors='coerce')
        elif mapping[i] in ['object']:
            continue
        else:
            print('Columns {} not set in the int, float, datetime or object type'.format(i,mapping[i]))
            continue
        print('Columns {} Finished! transform to {}'.format(i,mapping[i]) )

    return df

def f_RenameDuplicate(df):
    dup_col_name = df.columns[df.columns.duplicated()]
    for i in dup_col_name:
        n = df[i].shape[1]
        dic = {i:[i+'_'+str(j)  for j in range(n)]}
        df = df.rename(columns=lambda c: dic[c].pop(0) if c in dic.keys() else c)
    
    return df

def f_NASummary(df):
    columns = df.columns
    result = []
    for i in df.columns:
        
        df_temp = df[i]
        N = len(df_temp)
        isna = df_temp.isna().sum()
        n_nonna = N - isna
        dtype = df_temp.dtype
        num_unique = df_temp.nunique()
        
        try:
            len_max = df_temp.str.len().max()
        except:
            len_max = 1
        
        result.append((i,dtype,num_unique,len_max,isna,N,n_nonna ,isna/(N)))
    
    df = pd.DataFrame(result,columns=['ColumnsName','dtype','nunique','data_len_max','NA_num','Total_length', 'length','NA_ratio'])#.sort_values('NA_ratio',ascending=False).reset_index().rename({'index':'Original'})
    return df

def f_STKNASummary(df,Tresh):
    Drop_company = list(df.query('Num > {}'.format(Tresh))['ID'].values)
    Fillna_company =  list(df.query('Num <= {}'.format(Tresh))['ID'].values)
                      
    return [Drop_company,Fillna_company]

def f_DropNa(df,missing_ratio, keep = []):
    col = df.columns
    
    df_ratio = df.isna().sum()/len(df)
    col_ratio =  df_ratio[df_ratio>=missing_ratio].index
    col_keep = [i for i in col_ratio if i in keep]
    col_drop = [i for i in col_ratio if i not in keep]
    df = df.drop(columns = col_drop)
    col_new = df.columns
    
    
    print('缺值比率為{}\n共有: {}個\n需保留keep的: {} 個\n最終須刪掉欄位數: {}'.format(missing_ratio,len(col_ratio),len(col_keep),len(col_drop)))
    print('剩餘欄位數: {}'.format(len(df.columns)))
    return df, col_drop

def f_pivotfindate(df,col_name,col_date):
    # 抓取各間公司的財報「起始日」以及「終止日」，
    df_groupby = df.groupby(col_name).agg({col_date:[min, max, 'unique'],}).reset_index()
    df_groupby.columns = [i[0]+'_'+i[1] if i[1] != '' else i[0]  for i in df_groupby.columns ]
    # df_groupby.to_csv(os.path.join(OutputFolder,'FIN_groupby.csv'),encoding = 'utf-8',index = False)
    return df_groupby

def f_labelQHA(df,col):
    
    df['Label'] = df[col].apply(lambda x :'Q' if 3 in set(pd.to_datetime(x).month) or 9 in set(pd.to_datetime(x).month) 
                                                else 'H' if 6 in set(pd.to_datetime(x).month) and 3 not in set(pd.to_datetime(x).month) and 9 not in set(pd.to_datetime(x).month) 
                                                else 'A')
    return df

def f_FinMissing(date_min,date_max,date_list,freq):
    
    frqmap = {'Q':'Q','H':'2Q','A':'Y'}
    ans = [list(set(pd.date_range(s,e,freq=frqmap[freq])) - set(pd.to_datetime(lis))) for s,e,lis,freq in zip(date_min, date_max, date_list, freq)]
    ans = [[ i.strftime('%Y/%m/%d')  for i in x]  for x in ans]
    
    return ans
    
def f_StkMissing(df,start,end):
    df = df[(df.index>=start) & (df.index<=end)]
    Null = df[df.isnull()]
    return Null



def df_O_unique(DF):
    for colname in list(DF):
        if DF[colname].dtype == 'O':  # 'O': Object/String
            print(colname, ':', len(DF[colname].unique()), '\n', DF[colname].unique(), '\n')
            
def df_NotO_unique(DF):
    for colname in list(DF):
        if DF[colname].dtype != 'O':  # 'O': Object/String
            print(colname, ':', len(DF[colname].unique()), '\n', DF[colname].unique(), '\n')

def df_i_unique(DF):
    for colname in list(DF):
        if DF[colname].dtype == 'int64':  # 'int64': Integer
            print(colname, ':', len(DF[colname].unique()), '\n', DF[colname].unique(), '\n')

def df_f_unique(DF):
    for colname in list(DF):
        if DF[colname].dtype == 'float64':  # 'float64': Float
            print(colname, ':', len(DF[colname].unique()), '\n', DF[colname].unique(), '\n')

def df_D_unique(DF):
    for colname in list(DF):
        if DF[colname].dtype == '<M8[ns]':  # '<M8[ns]': Date
            print(colname, ':', len(DF[colname].unique()), '\n', DF[colname].unique(), '\n')

def df_unique(DF):
    for colname in list(DF):
        print(colname, ':', len(DF[colname].unique()), '\n', DF[colname].unique(), '\n')
        
def df_unique_num(DF):
    for colname in list(DF):
        print(colname, ':', len(DF[colname].unique()))
        

def df_joinTable(df1, df2, columns = None):
    
    if columns is None:
        return df1.join(df2)
    else:
        return df1.join(df2[columns])



## 資料清理
import csv



def f_DropOutput(N_b, N_2, N_1,drop ,filename = 'Drop.csv'):
    
    r = N_2/N_1 *100
    
    with open(f_PathFileName('Output',filename),'a',newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerows([(N_b - N_2,N_2,drop)])
    
    print('Drop       : {:>8} ({:.2f}%)'.format(N_b - N_2, (N_b - N_2)/N_1 * 100))
    print('New Data   : {:>8} ({:.2f}%)'.format(N_2,r))
    
    if len(drop) > 10:
        print('Detail     : {}...{}'.format(drop[:5],drop[-5:]))
    else:
        print('Detail     : {}'.format(drop))
    
    return 0
    

def f_DropUnEqual(df,column,length,N_1,fun = [len]):
    
    for i,j,k in zip(column,length,fun):
        N_b = len(df)
        #print('\nDropping column {} unequal to {}....'.format(i,j))
        if k is None:
            drop = df.loc[df[i] !=j]['CASE_ID'].unique()
            df = df.loc[df[i] ==j]             
        else:

            drop = df.loc[df[i].apply(len) !=j]['CASE_ID'].unique()
            df = df.loc[df[i].apply(k) ==j] 

        N_2 = len(df)
        f_DropOutput(N_b, N_2, N_1,drop)
    return df



def f_DropOtherThanNum(df,column,N_1):
    N_b = len(df)
    idx_isalpha = df[column].str[0].str.isalpha()
    drop = df[idx_isalpha]['CASE_ID'].unique()
    df = df.loc[~idx_isalpha]
    
    N_2 = len(df)
    
    f_DropOutput(N_b, N_2, N_1,drop)
    
    return df

def f_DropPosition(df,column,str_idx,values,N_1):

    for i,j,k in zip(column,str_idx,values):
        N_b = len(df)
        #print('\nDropping {}[{}] == {}'.format(i,j,k))
        idx = df[i].str[j] == k
        drop = df[idx]['CASE_ID'].unique()
        df = df.loc[~idx]

        N_2 = len(df)
        f_DropOutput(N_b, N_2, N_1,drop)
        
    return df


def f_DropPeriod(df,start,end,N_1):
    N_b = len(df)
    new = [ str(x)[-2:] for x in  range(start,end)]
    idx = df['CASE_ID'].str.startswith(tuple(new))
    drop = df[~idx]['CASE_ID'].unique()
    df = df.loc[idx]
    
    N_2 = len(df)
    
    f_DropOutput(N_b, N_2, N_1,drop)
    
    return df

def f_CountSummary(df,col):
    
    N = len(df)
    df = pd.DataFrame(df.groupby(col)[col].count())
    df['Ratio'] = df[col] / N
    return df


def f_toMapping(df,keys,values):
    ans = {}
    for i,j in zip(df[keys],df[values]):
        ans.update({i:j})
        
    return ans


def f_LangDetect(df): # df a series
    import langid
    import numpy as np
    
    if pd.api.types.is_numeric_dtype(df):
        return 'number'
    elif pd.api.types.is_datetime64_any_dtype(df):
        return 'datetime'
    else:
        try:
            return langid.classify(','.join(df.dropna().to_list()))[0]
        except Exception as e:
            return e

        
def translate(df):
    import string
    punct = string.punctuation.replace('-', '').replace('<', '').replace('>', '').replace('.','')
    transtab = str.maketrans(dict.fromkeys(punct, ''))

    return df.str.translate(transtab)

def f_StrFormat(df,values_from,value_to):
    df = translate(df.str.strip()).replace(values_from,value_to)
    return df.values

def f_groupbySummary(df,columns,outputfile):
    
    file = f_PathFileName(OutputFolder ,outputfile)
    with pd.ExcelWriter(file,mode="A") as writer:
        for column in columns:
            df_groupby_emplnote = df.fillna({column:'-1'}).groupby(column).agg({'CASE_ID':['nunique','unique']})
            df_groupby_emplnote.loc[:,('CASE_ID','percent')] = df_groupby_emplnote.loc[:,('CASE_ID','nunique')] / len(df)
            df_groupby_emplnote = df_groupby_emplnote.sort_values(('CASE_ID','percent'),ascending=False)
            df_groupby_emplnote = df_groupby_emplnote[[('CASE_ID','nunique'),('CASE_ID','percent'),('CASE_ID','unique')]]
            display(df_groupby_emplnote)
            

