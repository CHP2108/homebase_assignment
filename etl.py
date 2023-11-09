from ucimlrepo import fetch_ucirepo 
import pandas as pd
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine


format_time_str = "%Y-%m-%d %H:%M:%S"

def extract_data() -> pd.DataFrame:
    # fetch dataset 
    print(f"{datetime.now().strftime(format_time_str)} -- STARTING EXTRACT DATA!!!")

    heart_disease = fetch_ucirepo(id=45) 
  
    # data (as pandas dataframes) 
    X = heart_disease.data.features 
    y = heart_disease.data.targets 
    
    # metadata 
    df = pd.concat([X, y], axis=1)
    print(f"{datetime.now().strftime(format_time_str)} -- END EXTRACT DATA!!!")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # fetch dataset 
    print(f"{datetime.now().strftime(format_time_str)} -- STARTING TRANSFORM DATA!!!")
    df_transformed = df.copy()
    df_transformed['sex'] = df['sex'].apply(lambda x: 'male' if x == 1 else 'female')
    df_transformed['cp'] = df['cp'].apply(lambda x: 'typical angina' if x == 1 \
                                        else ("atypical angina" if x == 2 \
                                        else ("non-anginal pain" if x ==3 \
                                        else ("asymptomatic" if x ==4 else "unknown"))))
    df_transformed['fbs'] = df['fbs'].apply(lambda x: True if x == 1 else False)
    df_transformed['restecg'] = df['restecg'].apply(lambda x: 'normal' if x == 0 \
                                            else ('having ST-T wave abnormality (T wave inversions and/or ST elevation or depression of > 0.05 mV)' if x == 1 \
                                            else ("showing probable or definite left ventricular hypertrophy by Estes' criteria" if x == 2 \
                                            else "unknown")))
    df_transformed['exang'] = df['exang'].apply(lambda x: 'yes' if x == 1 else 'no')
    df_transformed['slope'] = df['slope'].apply(lambda x: 'upsloping' if x == 1 \
                                            else ('flat' if x == 2 \
                                            else ("downsloping" if x == 3 \
                                            else "unknown")))
    df_transformed['thal'] = df['thal'].apply(lambda x: 'normal' if x == 3 \
                                            else ('fixed defect' if x == 6 \
                                            else ("reversable defect" if x == 7 \
                                            else "unknown")))
    # Generate summary statistics
    print("Check null value \n",df_transformed.isna().sum())
    print("Check duplicate", df_transformed.duplicated().sum())
    summary_stats = df_transformed.describe()

    # Display the summary statistics
    print(summary_stats)
    print(f"{datetime.now().strftime(format_time_str)} -- END TRANSFORM DATA!!!")
    return df_transformed

def load_data(df: pd.DataFrame) -> None:
    db_user = "admin"
    db_password = "admin"
    db_host = "localhost"
    db_port = 5432
    db_name = "homebase_db"

    print(f"{datetime.now().strftime(format_time_str)} -- STARTING LOAD DATA!!!")
    conn = psycopg2.connect(database = db_name, 
                            user = db_user, 
                            host= db_host,
                            password = db_password,
                            port = db_port)
    cur = conn.cursor()
#     cur.execute("""
# CREATE TABLE heart_disease (
# age INTEGER,
#     sex VARCHAR(255),
#     cp VARCHAR(255),
#     trestbps INTEGER,
#     chol INTEGER,
#     fbs VARCHAR(255),
#     restecg VARCHAR(255),
#     thalach INTEGER,
#     exang VARCHAR(255),
#     oldpeak INTEGER,
#     slope VARCHAR(255),
#     ca INTEGER,
#     thal VARCHAR(255),
#     num INTEGER
# );
# """)
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    df.to_sql('heart_disease', engine, index=False, if_exists='replace')

    cur.execute("SELECT * FROM heart_disease;")
    data = cur.fetchall()
    print("Length Data in the heart_disease table:", len(data))

    # Close communication with the database
    cur.close()
    conn.close()
    print(f"{datetime.now().strftime(format_time_str)} -- END LOAD DATA!!!")


if __name__ == "__main__":
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)
