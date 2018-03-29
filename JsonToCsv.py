import pandas as pd
import os
import json
import csv

class JsonToCsv():
    def __init__(self):
        print('Create JsonToCsv Object')
        
    def identify_nested_lists(self,data):
        nest_cols=[columns for columns in data.columns if isinstance(data[columns].any(),list)]
        #print(nest_cols)
        return nest_cols
    
    def flatten_nested_data(self,data):
        nested_headers=self.identify_nested_lists(data)
        flattened_data=data
        for header in nested_headers:
            try:
                #print(header+' '+str(data[header][0]))
                #print(header)
                flat_data=pd.DataFrame()
                count=0
                for index,row in flattened_data.iterrows():
                    nested_column=row[header]
                    row_data=pd.DataFrame(row).transpose()
                    nested_flat_data=pd.DataFrame()
                    if isinstance(nested_column,list) and len(nested_column)>0:
                        for details in nested_column:
                            flatten_details=pd.io.json.json_normalize(details)
                            flatten_details.columns = [header+'.'+str(col) for col in flatten_details.columns]
                            nested_flat_data=nested_flat_data.append(flatten_details)
                            #print(row_data.index.values)
                            #print(flatten_details.index.values)
                            #print('row shape '+str(row_data.shape)+'flatten details shape '+str(flatten_details.shape))
                        nested_flat_data=self.flatten_nested_data(nested_flat_data)
                        #print(nested_flat_data.shape)
                        row_data=pd.concat([row_data]*nested_flat_data.shape[0])
                        _flat_data=pd.concat([row_data.reset_index(drop=True),nested_flat_data.reset_index(drop=True)],axis=1)
                        #print('concated data shape '+str(_flat_data.shape))
                        flat_data=pd.concat([flat_data,_flat_data],axis=0)
                        count+=len(nested_column)
                        #print(str(len(nested_column))+' '+str(count)+' '+str(flat_data.shape))
                    else:
                        count+=1
                        flat_data=pd.concat([flat_data,row_data],axis=0)
                        #print('0 - No record found '+str(count)+' '+str(flat_data.shape))
                flattened_data=flat_data.drop(header,axis=1)
                #print(flattened_data.shape)
                #print(flat_data.shape)
            except KeyError:
                print('keyerror'+header)
        return flattened_data

    def convert_epochtime(self,csv_data):
        time_cols=[columns for columns in csv_data.columns if columns.endswith('At')]
        for time_col in time_cols:
            csv_data[time_col]=pd.to_datetime(csv_data[time_col],unit='ms')
        return csv_data
        
    def flatten_data(self,file_name):
        #print('Converting file - '+file_name)
        _json_data=json.loads(open(file_name,encoding='utf-8').read())
        csv_data = pd.io.json.json_normalize(_json_data)
        csv_data = self.flatten_nested_data(csv_data)
        return csv_data
        
def main():
    print('Enter file path or folder path where all json files are present:')
    fpath=input()
    jtc=JsonToCsv()
    if os.path.isfile(fpath):
        print('file')
        csv_data=jtc.flatten_data(fpath)
        csv_data.to_csv(fpath+'.csv',encoding='utf-8')
    """
	elif os.path.isdir(fpath):
        print('dir')
        final_csv=pd.DataFrame()
        print('Enter json file extension:')	
        extn=input()
        m_ind='Y'
        print('Do you wish to merge all output csv to single csv: [Y]/N')
        ind=input()
        if ind=='N' and ind!='Y':
            m_ind=ind
        os.chdir(fpath)
        cwd = os.getcwd()
        #itereate thorough folders to read json files
        folder_count=0
        for root, dirs,files in os.walk(cwd):
            files = [ fi for fi in files if fi.endswith(extn) ]
            for name in files:
                file_name=os.path.join(root, name)
                csv_data=jtc.flatten_data(fpath)
                csv_data.to_csv(file_name+'.csv',encoding='utf-8')
                print('csv shape - '+str(csv_data.shape))
                final_csv=final_csv.append(csv_data)
                print('Final csv shape after append'+str(final_csv.shape))
        final_csv = jtc.convert_epochtime(final_csv)
        final_csv.to_csv('final_output.csv',encoding='utf-8')
		"""
    else:
        print("Please enter a valid file path")

if __name__== "__main__":
    main()