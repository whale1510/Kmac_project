## 전처리 코드 모듈 + 임계값 설정 모듈

#라이브러리 로드
import os
import json
import datetime
import pandas as pd



# 전처리 함수
def json2csv(folder_path):
    df_list = []
    file_path_list = os.listdir(folder_path)
    for file in file_path_list:
        file = os.path.join(folder_path, str(file))
        with open(file, 'r') as f:
            data = json.load(f)
            # json 데이터를 pandas DataFrame으로 변환
            df = pd.DataFrame(data)
            df_list.append(df)


    # 모든 데이터프레임을 하나로 통합
    merged_df = pd.concat(df_list, ignore_index=True)

    return merged_df

def check_and_prompt(message):
    print(message)
    user_input = input("Continue? (y/n): ").strip().lower()
    if user_input != 'y':
        print("Program stopped.")
        raise SystemExit 

def preprocessing(df):
    #결측치 검사
    null_values = df.isnull().sum()
    if null_values.any():
        check_and_prompt("Null values in the following columns:\n" + str(null_values[null_values > 0]))

    # 타입 검사
    print("\nData types of each column:")
    print(df.dtypes)

    # 열 데이터 타입 검사
    for column in ['x', 'y', 'z', 'user']:
        if not pd.api.types.is_numeric_dtype(df[column]):
            check_and_prompt(f"{column} column is not numeric. Please check the data.")

    # 중복 검사
    duplicate_rows = df.duplicated().sum()
    if duplicate_rows > 0:
        check_and_prompt(f"\nNumber of duplicate rows: {duplicate_rows}")

    # 날짜 타입으로 변환 및 검사
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    if df['time'].isnull().any():
        check_and_prompt("\nSome time values could not be converted to datetime.")

    # 데이터프레임 타입 검사
    print("\nData types after conversion:")
    print(df.dtypes)

    return df

def append_to_csv(df, user_folder):
    for user, group in df.groupby('user'):
        user_folder_name = os.path.join(user_folder, str(user))
        os.makedirs(user_folder_name, exist_ok=True)

        user_file = os.path.join(user_folder_name, f'{user}.csv')
        # 파일이 없다면 생성, 있다면 데이터를 덧붙임
        if not os.path.exists(user_file):
            group.to_csv(user_file, index=False)
        else:
            group.to_csv(user_file, mode='a', header=False, index=False)
            group = pd.read_csv(user_file)
            # 'date' 변수 기준으로 데이터 정렬
            group = group.sort_values(by='time')
            group = group.drop_duplicates()
            group.to_csv(user_file, index=False)
    
    file_list = os.listdir(user_folder) 
    
    return file_list

# 임계값 생성 함수 /
def determine_threshold(df, user_folder, user_folder_list):
    for user in user_folder_list:
        user_history_file = os.path.join(user_folder, user, f'{user}.csv')
        threshold_file_path = os.path.join(user_folder, user, f'{user}_threshold.csv')

        data = pd.read_csv(user_history_file)

        data['time'] = pd.to_datetime(data['time'], errors='coerce')
        data['date'] = data['time'].dt.date

        data['abs_sum'] = data[['x', 'y', 'z']].abs().sum(axis=1)

        daily_abs_sum = data.groupby('date')['abs_sum'].sum().reset_index()

        mean_activity = daily_abs_sum['abs_sum'].mean()
        std_dev_activity = daily_abs_sum['abs_sum'].std()

        # 하한 이상치 기준 계산
        k = 2
        lower_bound = mean_activity - (k * std_dev_activity)

        # 데이터 JSON 파일로 저장 (덮어쓰기)
        with open(threshold_file_path, 'w') as json_file:
            json.dump({"threshold": lower_bound}, json_file, indent=4)



if __name__ == "__main__":
    ## 변수 및 path 준비과정
    # today filename 
    now = datetime.datetime.now()
    current_date = now.replace(year=2024).strftime('%Y%m%d')

    # load config
    base_path = os.getcwd()

    config_file_path = "train_config.json"
    with open(config_file_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    # 폴더 위치를 가져옵니다.
    Gyro_train_folder_path = config['input_json_file_path']["Gyro_train_data"]

    #통합 데이터 1차 저장경로
    total_filename = "Train_Total.csv"
    csv_output_path = (config['json_to_csv_path']['csv_file'] + total_filename)

    #유저별 데이터 폴더 생성 및 저장경로
    user_folder = config['user_file_path']['user_file'] 
    


    ## 전처리 과정
    #날짜별로 통합(데이터셋에 따라 맞춰서 구축)
    pass

    merged_df = json2csv(Gyro_train_folder_path)

    #통합 데이터 전처리
    total_df = preprocessing(merged_df)

    #통합 데이터 저장
    total_df.to_csv(csv_output_path)

    ##분류 및 임계값 생성 과정
    #ID별 폴더 생성 및 파일 생성
    user_folder_list = append_to_csv(total_df, user_folder)

    #각 파일 위치에 이상치 저장
    determine_threshold(total_df, user_folder, user_folder_list)


