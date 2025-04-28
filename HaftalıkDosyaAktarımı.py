import psycopg2 
from sqlalchemy import create_engine
import pandas as pd
import os
import datetime

# Veritabanı bağlantı bilgileri
host = 'localhost'
database = 'postgres'
user = 'postgres'
password = '' --Database Kullanıcı Şifreniz.
port = '5432'

# PostgreSQL veritabanına bağlan
conn = psycopg2.connect(
host=host,
database=database,
user=user,
password=password,
port=port
)

# Kolon içeriklerini düzelt
class Utils:
    
    @staticmethod
    def cleanText(text:str): 
        if (text.lower()  not in ['nan', '', '.', 'tbc','0',' ','NaN','NaT','nat'] or text is not None ):
            return text
        else : 
            return ""

    @staticmethod
    def getRoot_dir():
        return  os.path.dirname(os.path.abspath(__file__))
    
    @staticmethod
    def getFullFilePath(fileName):
        return  Utils.getRoot_dir() +'/' +fileName
    

turkish_to_english = {
    'ç': 'c',
    'ğ': 'g',
    'ı': 'i',
    'ö': 'o',
    'ş': 's',
    'ü': 'u',
}

def fix_column_names(df):
    for orjCol in df.columns:
        newColumName = orjCol.lower().strip()
        for turkish_char, english_char in turkish_to_english.items():
            newColumName = newColumName.replace(turkish_char, english_char)
        
        newColumName = newColumName.replace(" ", "_")
        newColumName = newColumName.replace("(", "")
        newColumName = newColumName.replace(")", "")
        newColumName = newColumName.replace("-", "")
        newColumName = newColumName.replace("__", "_")
        newColumName = newColumName.replace("%", "_perc")
        newColumName = newColumName.replace("col_", "")
        
        df.rename(columns={orjCol: newColumName}, inplace=True)
        # type changes
        df[newColumName] = df[newColumName].fillna('')
        df[newColumName] = df[newColumName].astype(str)
        df[newColumName] = df[newColumName].apply(Utils.cleanText)
        
try:
        

    now = datetime.datetime.now() 
    

    WeekStartDate= now + datetime.timedelta(days = -now.weekday())
    
    Last_Week = WeekStartDate+pd.DateOffset(days=-7)
    year_month = Last_Week.strftime("%Y%m")
    year, week_number, day_of_week = Last_Week.isocalendar()
    print(week_number)

    folder_path = r"D:\İlgili Klasör Yolu" 
    file_name = "Excell Adı" + str(year_month[2:]) + "_W" + str(week_number) + ".xlsx"

    #file_name  = "ExcellDoysasınınAdı.xlsx" --Bu Manuel Güncelleme yaparsanız diye elde tutulan bir dosya yolu

    file_path = os.path.join(folder_path, file_name)

    print("file path: " + file_path)   


    df = pd.read_excel(file_path, skiprows=2)  -- İlgili excell dosyasını oku ve  içinden 2 satır atlayarak almaya başka DF içine at
    df['year'] = str(year)  ---Excellin kolonları haricinde sona bir kolon daha oluştur buna year ver dedik 
    df['week_number'] = str(week_number)  -- Aynı Şekilde buna hafta numarasını  yazdırdık 

    fix_column_names(df)  -- Yukarda tanımlı methodu çağırıp excellin kolonlarını duzenledik Türkçe karakterleri boşlukları vs.


    from psycopg2 import sql 
    cursor = conn.cursor()
    delete_fact_query = sql.SQL("delete  from \"TESTTablo\"  where \"week_number\" = \'" + str(week_number) + "\' and \"year\" = \'" + str(year) + "\'") -- Test Tablosunda o haftada veri varsa sildirdik.
    conn.autocommit = True
    cursor.execute(delete_fact_query)
    conn.commit()

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    df.to_sql('TesTTablo', engine, if_exists='append', index=False)
    conn.close()
    print('END Process')



except Exception as e:
    now = datetime.datetime.now()
    date_string = now.strftime("%Y%m%d_%H%M")
    file_name = f"TestVeriHatası_{date_string}.txt"
    file_path = os.path.join("D:\ErrorDosyası\\", file_name)
    with open(file_path, "w") as f:
        f.write(str(e))