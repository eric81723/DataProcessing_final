import pandas as pd
import os
from Eric_utilites.Eric_DataProcessing import *

# 資料輸出位置, 將任何需要輸出之資料放於此
Outputfolder = 'Output'

# 資料輸入位置, 將資料放於此進行輸入
# 檔案包含: 
Inputfolder = 'Input'

col = ['CASE_ID','FIN_REPT','CASEBASE1_OPENY1','CASEBASE1_OPENY2','CASEBASE1_OPENY3',
       'CASEBASE1_COUNTY1','CASEBASE1_COUNTY2','CASEBASE1_COUNTY3',
       'CASEFINANCIAL_9_FINDATE1','CASEFINANCIAL_9_FINDATE2','CASEFINANCIAL_9_FINDATE3',
       'CASEFINANCIALM_9_FINDATE1','CASEFINANCIALM_9_FINDATE2','CASEFINANCIALM_9_FINDATE3',
       'CASEBASE1_ISMERGER1','CASEBASE1_ISMERGER2','CASEBASE1_ISMERGER3',
       'CASEBASE2_TAXTYPE', 'CASEBASE2_TAXY1','CASEBASE2_TAXY2','CASEBASE2_TAXY3',
       'CASEBASE2_CPATYPE', 'CASEBASE2_CPAY1','CASEBASE2_CPAY2','CASEBASE2_CPAY3',
       'CASEBASE2_COMPTYPE', 'CASEBASE2_COMPY1','CASEBASE2_COMPY2','CASEBASE2_COMPY3',
       'CASEBASE2_CPATYPEM', 'CASEBASE2_CPAYM1','CASEBASE2_CPAYM2','CASEBASE2_CPAYM3',
       'CASEBASE2_COMPTYPEM', 'CASEBASE2_COMPYM1','CASEBASE2_COMPYM2','CASEBASE2_COMPYM3',
       'CASEBASE2_FINDATE1','CASEBASE2_FINDATE2','CASEBASE2_FINDATE3',
       'CASEBASE2_FINDATEM1','CASEBASE2_FINDATEM2','CASEBASE2_FINDATEM3']



def f_DropPipline(df):
    N_raw = len(df)
    print('Raw data : {} ({:.2f}%) rows\n'.format(N_raw,N_raw/N_raw *100))
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))

    ## 刪除 CASE_ID　不是十碼　的資料
    ## 刪除 進行中、退件、以及中止之案件編號 
    ##   - 2020.6.23 09:59 a.m. 幸儒 信件中提到, 不需考慮退件
    ##   - 2020.6.23 10:24 a.m. 小高 案件狀態參見 newccis.dbo.CCASE.CASE_CLOSE 0:作業中 1:結案, 2:中止, 3: ?, 4:退件
    ##   - 2020.6.23 11:19 a.m. 幸儒 提到只考慮 CASE_CLOSE = 1 的情況
    print('\nSTEP 1\nCASE_ID  長度不等於 10 碼的資料刪除狀況')
    df = f_DropUnEqual(df,['CASE_ID'],[10,],N_raw,[len])
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
          
    print('\nSTEP 2\n進行中、退件、以及中止之案件編號資料刪除狀況')
    df = f_DropUnEqual(df,['CASE_CLOSE'],["1"],N_raw,[None])
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))

    
    ## 刪除 CASE_ID 開頭為英文的案件變號
    ##  - 2020.6.23 09:59 a.m. 幸儒 信件中提到 「前面有英文字的案件編號為早期手寫報告時代的編號不應該納入」
    print('\nSTEP 3\nCASE_ID 開頭為英文的資料刪除狀況')
    df = f_DropOtherThanNum(df,'CASE_ID',N_raw)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    ## 刪除 CASE_ID 倒數第五碼為 5 or 7 的案件編號
    ##  - 2020.6.23 09:59 a.m. 幸儒 信件中提到 「案件標號倒數第五碼為7及5為特殊案件，不應該納入」    
    print('\nSTEP 4\n案件編號倒數第五碼為 5 的刪除狀況')
    df = f_DropPosition(df,['CASE_ID'],[-5],["5"],N_raw)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    
    print('\nSTEP 5\n案件編號倒數第五碼為 7 的刪除狀況')
    df = f_DropPosition(df,['CASE_ID'],[-5],["7"],N_raw)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    
    
    ## 只抓取 2008~2020 年之間的案件編號
    ##  - 2020.6.23 09:59 a.m. 幸儒 於電話中提到, 「2008 年以前報告與現行徵信報告有所不同, 因此只考慮 2008 年以後」  
    print('\nSTEP 6\n##抓取 2008~2020年之間的案件編號')
    df = f_DropPeriod(df,2008,2020,N_raw)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    ## 刪除沒有 CP_ID 的資料 
    ## - 2020.7.3 這些案件報告別是 8 代表非正常報告<
    print('\nSTEP 7\n刪除沒有 CP_ID 的資料(該類案件別為 8, 尚未了結合處標記該數值)')
    N_b = len(df)
    drop = list(df[df['CASEBASE_CP_ID'].isna()]['CASE_ID'])
    df = df.dropna(subset=['CASEBASE_CP_ID'], inplace=False)
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    
    ## 刪除 FIN_REPT = 8 的資料
    print('\nSTEP 8\n刪除案件別為 8, 但留第 5, 6 碼為 86 與 81 之資料')
    N_b = len(df)
    idx_8_delete = (df['FIN_REPT'] == 8) &  ((df['CASE_ID'].str[4:6] != '86') & (df['CASE_ID'].str[4:6] != '81' ))
    drop = list(df[idx_8_delete]['CASE_ID'])
    df = df.loc[~idx_8_delete]
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    ## 2020.07.13僅留 FIN_REPT = 2,3,6,8 的資料
    print('\nSTEP 8\n僅留 FIN_REPT = 2,3,6,8 的資料')
    N_b = len(df)
    idx_delete = df['FIN_REPT'].apply(lambda x : x not in [2,3,6,8])
    drop = list(df[idx_delete]['CASE_ID'])
    df = df.loc[~idx_delete]
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
          
    df['CASE_Y'] = pd.to_numeric(df['CASE_ID'].str[:2])+2000 #年度
    df['CASE_M'] = pd.to_numeric(df['CASE_ID'].str[2:4])     #月份
    
    ## 2020/07/13 將不符合情況的近一年營業收入資料刪除
    print('\nSTEP 9 刪除案件年度有錯誤之狀況')
    N_b = N_2
    drop = f_ReptYearErr(df)
    idx_delete = df['CASE_ID'].apply(lambda x : x in drop)
    df = df.loc[~idx_delete]
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    ## 2020/07/13 相差不為 1 的案件編號
    print('\n相差不為 1 的案件編號')
    N_b = N_2
    drop = f_YearCHK(df)
    idx_delete = df['CASE_ID'].apply(lambda x : x in drop)
    df = df.loc[~idx_delete]
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    ## 2020/07/13 年分 outlier
    print('\n年分 outlier')
    N_b = N_2
    drop = f_YearOutLier(df,2020)
    idx_delete = df['CASE_ID'].apply(lambda x : x in drop)
    df = df.loc[~idx_delete]
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
        
    
    ## 2020/07/13 年分 順序錯誤
    print('\n年分 順序錯誤')
    N_b = N_2
    drop = f_ErrorOrder(df)
    idx_delete = df['CASE_ID'].apply(lambda x : x in drop)
    df = df.loc[~idx_delete]
    N_2 = len(df)
    f_DropOutput(N_b, N_2, N_raw,drop)
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    N_final = len(df)
    
    df = df.reset_index().drop(columns = 'index',inplace=False)
    print('-----------------------------------------------------------------------------------------')
    print('\nTotal Drop : {:>8} ({:.2f}%)'.format(-N_final+N_raw, 100 - N_final/N_raw * 100))
    print('New Data   : {:>8} ({:.2f}%)'.format(N_final,N_final/N_raw * 100))
    print('CP_ID      : {:>8}'.format(df['CASEBASE_CP_ID'].nunique()))
    return df


### 找出 年份 outlier
def f_YearOutLier(df,year):
    y_col = ['CASEBASE1_OPENY1','CASEBASE1_OPENY2','CASEBASE1_OPENY3',
       'CASEBASE2_TAXY1','CASEBASE2_TAXY2','CASEBASE2_TAXY3',
       'CASEBASE2_CPAY1','CASEBASE2_CPAY2','CASEBASE2_CPAY3',
       'CASEBASE2_COMPY1','CASEBASE2_COMPY2','CASEBASE2_COMPY3',
       'CASEBASE2_CPAYM1','CASEBASE2_CPAYM2','CASEBASE2_CPAYM3',
       'CASEBASE2_COMPYM1','CASEBASE2_COMPYM2','CASEBASE2_COMPYM3',
       'CASEBASE2_FINDATE1','CASEBASE2_FINDATE2','CASEBASE2_FINDATE3',
       'CASEBASE2_FINDATEM1','CASEBASE2_FINDATEM2','CASEBASE2_FINDATEM3']
    
    CASE_year_out = pd.DataFrame(columns=['CASE_ID','date','Columns'])
    
    for i in y_col:
        if df[i].dtype == float or df[i].dtype == int:
            year = year
            temp = df.query('{} >= {}'.format(i,year))[['CASE_ID',i]]
            temp = temp.rename(columns = {i:'date'})
            temp['Columns'] = i
            if len(temp) != 0:
                CASE_year_out = CASE_year_out.append(temp)
        else:
            year = str(year)
            temp = df.query('{} >= "{}"'.format(i,year))[['CASE_ID',i]]
            temp = temp.rename(columns = {i:'date'})
            temp['Columns'] = i
            if len(temp) != 0 :
                CASE_year_out = CASE_year_out.append(temp)
    
    #display(CASE_year_out)
    file = f_PathFileName(Outputfolder ,'f_YearOutLier.xlsx')
    CASE_year_out.to_excel(file)
    CASE_list = CASE_year_out['CASE_ID'].to_list()
    return CASE_list

def f_YearCHK(df):
    Y_col = ['CASEBASE1_OPENY', 'CASEBASE2_FINDATE','CASEBASE2_FINDATEM']
    file = f_PathFileName(Outputfolder ,'f_YearCHK.xlsx')
    
    CASE_list = set()
    with pd.ExcelWriter(file,mode="A") as writer:
        for i in Y_col:
            y_1 = df[i+'1']
            y_2 = df[i+'2']
            y_3 = df[i+'3']
            dtype = y_1.dtype

            if dtype == object:
                id_1 = abs(pd.to_numeric(y_1) - pd.to_numeric(y_2)) > 1
                id_2 = abs(pd.to_numeric(y_2) - pd.to_numeric(y_3)) > 1 

            elif dtype == 'datetime64[ns]':
                id_1 = abs(y_1.dt.year - y_2.dt.year) > 1
                id_2 = abs(y_2.dt.year - y_3.dt.year) > 1

            else:
                id_1 = abs(y_1 -y_2) > 1
                id_2 = abs(y_2 -y_3) > 1

            idx_error = id_1 | id_2
            df_result = df[idx_error][col]
            
            #display(df_result)
            CASE_list = CASE_list | set(df_result['CASE_ID'])
            df_result.to_excel(writer, i)
    return sorted(list(CASE_list))
            
def f_ReptYearErr(df):
    Y_col = ['CASEBASE1_OPENY', 'CASEBASE2_FINDATE','CASEBASE2_FINDATEM']
    file = f_PathFileName(Outputfolder ,'f_ReptYearErr.xlsx')
    CASE_list = set()
    with pd.ExcelWriter(file,mode="A") as writer:
        for i in Y_col:
            print(i)
            y_1 = df[i+'1']
            y_2 = df[i+'2']
            y_3 = df[i+'3']
            dtype = y_1.dtype

            if dtype == object:
                idx1 = df['CASE_Y'] - pd.to_numeric(y_1) < 0
                idx2 = df['CASE_Y'] - pd.to_numeric(y_2) < 1
                idx3 = df['CASE_Y'] - pd.to_numeric(y_3) < 2

            elif dtype == 'datetime64[ns]':               
                idx1 = df['CASE_Y'] - y_1.dt.year < 0
                idx2 = df['CASE_Y'] - y_2.dt.year < 1
                idx3 = df['CASE_Y'] - y_3.dt.year < 2      
                
            else:                
                idx1 = df['CASE_Y'] - y_1 < 0
                idx2 = df['CASE_Y'] - y_2 < 1
                idx3 = df['CASE_Y'] - y_3 < 2               

            idx_error = idx1 | idx2 | idx3
            df_result = df[idx_error][col]
            
            #display(df_result)
            #df_result.to_excel(writer, i)
            
            CASE_list = CASE_list | set(df_result['CASE_ID'])
    return sorted(list(CASE_list))


def f_ErrorOrder(df):
    col = ['CASE_ID','FIN_REPT','CASEBASE1_OPENY1','CASEBASE1_OPENY2','CASEBASE1_OPENY3',
       'CASEBASE1_COUNTY1','CASEBASE1_COUNTY2','CASEBASE1_COUNTY3',
       'CASEFINANCIAL_9_FINDATE1','CASEFINANCIAL_9_FINDATE2','CASEFINANCIAL_9_FINDATE3',
       'CASEFINANCIALM_9_FINDATE1','CASEFINANCIALM_9_FINDATE2','CASEFINANCIALM_9_FINDATE3',
       'CASEBASE1_ISMERGER1','CASEBASE1_ISMERGER2','CASEBASE1_ISMERGER3',
       'CASEBASE2_TAXTYPE', 'CASEBASE2_TAXY1','CASEBASE2_TAXY2','CASEBASE2_TAXY3',
       'CASEBASE2_CPATYPE', 'CASEBASE2_CPAY1','CASEBASE2_CPAY2','CASEBASE2_CPAY3',
       'CASEBASE2_COMPTYPE', 'CASEBASE2_COMPY1','CASEBASE2_COMPY2','CASEBASE2_COMPY3',
       'CASEBASE2_CPATYPEM', 'CASEBASE2_CPAYM1','CASEBASE2_CPAYM2','CASEBASE2_CPAYM3',
       'CASEBASE2_COMPTYPEM', 'CASEBASE2_COMPYM1','CASEBASE2_COMPYM2','CASEBASE2_COMPYM3',
       'CASEBASE2_FINDATE1','CASEBASE2_FINDATE2','CASEBASE2_FINDATE3',
       'CASEBASE2_FINDATEM1','CASEBASE2_FINDATEM2','CASEBASE2_FINDATEM3']
    Y_col = ['CASEBASE1_OPENY', 'CASEBASE2_FINDATE','CASEBASE2_FINDATEM']
    file = f_PathFileName(Outputfolder ,'f_ErrorOrder.xlsx')
    CASE_list = set()
    with pd.ExcelWriter(file,mode="A") as writer:
        for i in Y_col:
            print(i)
            y_1 = df[i+'1']
            y_2 = df[i+'2']
            y_3 = df[i+'3']
            dtype = y_1.dtype

#             if dtype == object:
#                 idx1 = df['CASE_Y'] - pd.to_numeric(y_1) < 0
#                 idx2 = df['CASE_Y'] - pd.to_numeric(y_2) < 1
#                 idx3 = df['CASE_Y'] - pd.to_numeric(y_3) < 2

#             elif dtype == 'datetime64[ns]':               
#                 idx1 = df['CASE_Y'] - y_1.dt.year < 0
#                 idx2 = df['CASE_Y'] - y_2.dt.year < 1
#                 idx3 = df['CASE_Y'] - y_3.dt.year < 2      
                
#             else:                
#                 idx1 = df['CASE_Y'] - y_1 < 0
#                 idx2 = df['CASE_Y'] - y_2 < 1
#                 idx3 = df['CASE_Y'] - y_3 < 2               

            idx_error = ((y_1 < y_2) &((y_1 != '') & (y_2 !='' )) ) | ((y_2 < y_3) & ((y_2 !='' )&( y_3 !='')))
            df_result = df[idx_error][col]
            
            #display(df_result)
            df_result.to_excel(writer, i)
            
            CASE_list = CASE_list | set(df_result['CASE_ID'])
    return sorted(list(CASE_list))

def f_DropColumns(df, symbol, keep = [], regex=True):
    col = df.columns
    col_drop = col[col.str.contains(symbol,regex=regex)]
    col_keep = [i for i in col_drop if i in keep]
    col_drop_real = [i for i in col_drop if i not in keep]
    #print('{} 型式\n共有: {}個\n需保留keep的: {} 個\n最終須刪掉欄位數: {}'.format(symbol,len(col_drop), len(col_keep),len(col_drop_real)))
    
    #df_drop = df.drop(columns=col_drop_real)
    #print('剩餘欄位數: {}\n'.format(len(col) - len(col_drop_real)))
    return f_DropOutFormat(symbol, col, col_drop, keep)

def f_DropColPipe(condi, regex = True):
    ans = [j(i)  for i,j in zip(condi.keys(),condi.values())]
    df = pd.DataFrame(ans,columns=['Symbol','ColumnName','Total','符合該條件數目','符合該條件,需保留的','最終刪數目','刪除百分比'])
    total = df['ColumnName'].sum()
    print('需刪除欄位共:{}'.format(len(set(total))))
    return df, total
          
    
def f_DropHighCard(df, numbers, keep = []):
    col = df.columns 
    df_object = df.select_dtypes(object)
    columns = df_object.columns
    Nunique = df_object.nunique()
    col_drop = columns[Nunique>numbers]
    col_keep = [i for i in col_drop if i in keep]
    col_drop_real = [i for i in col_drop if i not in keep]
    
    return f_DropOutFormat('DropHighCard {}'.format(numbers), col, col_drop, keep)
    

def f_DropNa2(df,missing_ratio, keep = []):
    col = df.columns
    
    df_ratio = df.isna().sum()/len(df)
    col_drop =  df_ratio[df_ratio>=missing_ratio].index

    return f_DropOutFormat('DropNA {}'.format(missing_ratio), col, col_drop, keep)


def f_DropIdentity(df, numbers, keep = []):
    col = df.columns 

    Nunique = df.nunique()
    col_drop = col[Nunique==numbers]
    
    return f_DropOutFormat('Drop Identity', col, col_drop, keep)


def f_DropHighCorr(df, thresh, keep = []):
    import numpy as np
    col = df.columns
    
    
    # Create correlation matrix
    corr_matrix = df.corr().abs()
    # Select upper triangle of correlation matrix
    upper = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(np.bool))
    upper = upper[keep + upper.columns.drop(keep).to_list()]
    # Find index of feature columns with correlation greater than thresh
    col_drop = [column for column in upper.columns if any(upper[column] > thresh)]
    col_keep = [i for i in col_drop if i in keep]
    col_drop_real = [i for i in col_drop if i not in keep]
    
    return f_DropOutFormat('Corr > {}'.format(thresh), col, col_drop, keep)



def f_DropOutFormat(symb, col, col_drop, keep):
    col_keep = [i for i in col_drop if i in keep]
    col_drop_real = [i for i in col_drop if i not in keep]
    
    return (symb ,col_drop_real,len(col), len(col_drop), len(col_keep), len(col_drop_real), len(col_drop_real)/len(col))

    
    