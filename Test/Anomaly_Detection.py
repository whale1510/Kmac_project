## 전처리 코드 모듈 + 이상 탐지 코드 모듈

#라이브러리 로드
import os
import json
import shutil
import datetime
import pandas as pd



#전처리 함수
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

def load_userfile_info(user_folder):
    threshold_list = {}
    folder_list = os.listdir(user_folder)
    for folder_name in folder_list:
        folder_path = os.path.join(user_folder, str(folder_name))
        if os.path.isdir(folder_path):
            for file_name in os.listdir(folder_path):
                if file_name.endswith(".json") and folder_name in file_name:
                    file_path = os.path.join(folder_path, str(file_name))

                    # JSON 파일을 읽어 딕셔너리에 저장
                    with open(file_path, 'r') as json_file:
                        data = json.load(json_file)
                        threshold_list[folder_name] = data

    return folder_list, threshold_list

def move_files(source, destination, normal_user_ids):
    ##임시로 통합하도록 작성해놓음##
    source_file = os.path.join(source, "raw_data.json")
    destination_file = os.path.join(destination, "raw_data.json")
    with open(source_file, 'r') as f:
        source_data = json.load(f)
    with open(destination_file, 'r') as f:
        destination_data = json.load(f)
    for entry in source_data:
        if str(entry["user"]) in normal_user_ids:
            destination_data.append(entry)


    with open(destination_file, 'w') as f:
        json.dump(destination_data, f, indent=4)

    """
    for filename in normal_user_ids:
        file_path = os.path.join(source, str(filename))
        if os.path.isfile(file_path):
            # 파일을 이동하고 대상에 같은 이름의 파일이 있으면 덮어씁니다
            shutil.move(file_path, os.path.join(destination, str(filename)))
    """


#이상탐지 함수
def detection_and_savefile(df, threshold_list, day_output_path, history_output_path, current_date):
    normal_group = []
    outlier_group = []
    total_group = []
    
    for user, group in df.groupby('user'):
        threshold = threshold_list.get(user)
        total_sum = group[['x','y','z']].abs().sum().sum()
        if total_sum < 3.0 :
            outlier_group.append({"user_id" : f'{user}', "status" : "1"})
            total_group.append({"user_id" : f'{user}', "status" : "1"})
        else :
            normal_group.append({"user_id" : f'{user}', "status" : "0"})
            total_group.append({"user_id" : f'{user}', "status" : "0"})

    results = {
        "date" : current_date,
        "data" : total_group
    }

    filename = f"result_{current_date}.json"
    file_path = os.path.join(day_output_path, filename)

    # JSON 파일에 쓰기
    with open(file_path, 'w') as json_file:
        json.dump(results, json_file, indent=4)

    #히스토리파일 생성 및 추가하기
    for entry in total_group:
        entry["time"] = current_date
    
    total_df = pd.DataFrame(total_group)
    history_file = os.path.join(history_output_path, 'alarm_history.csv')

    if os.path.exists(history_file):
        total_df.to_csv(history_file, mode='a', header=False, index=False)
    else:
        total_df.to_csv(history_file, mode='w', header=True, index=False)

    #정상 그룹 파일명만 추출
    normal_user_ids = [entry["user_id"] for entry in normal_group]
    return normal_user_ids
 
    #날짜별 분류 가능하게 변경할 예정
    
    
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



if __name__ == "__main__":
    ## 변수 및 path 준비과정
    # today filename 
    now = datetime.datetime.now()
    current_date = now.replace(year=2024).strftime('%Y%m%d')

    # load config
    base_path = os.getcwd()

    config_file_path = "test_config.json"
    with open(config_file_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    # 폴더 위치를 가져옵니다.
    Gyro_test_folder_path = config['input_json_file_path']["Gyro_test_data"]
    Gyro_train_folder_path = config['input_json_file_path']["Gyro_train_data"] 
 

    #통합 데이터 1차 저장경로
    total_filename = "Test_Total.csv"
    csv_output_path = (config['json_to_csv_path']['csv_file'] + total_filename)      

    #유저별 데이터 접근 및 로드 경로
    user_folder = config['user_file_path']['user_file']

    #결과 저장 경로
    day_output_path = config['output_path']['day_output_result']
    history_output_path = config['output_path']['history_output_result']

    ##전처리 과정
    #날짜별로 통합(데이터셋에 따라 맞춰서 구축)
    pass

    merged_df = json2csv(Gyro_test_folder_path)

    #통합 데이터 전처리
    total_df = preprocessing(merged_df)

    #통합 데이터 저장
    total_df.to_csv(csv_output_path)



    ##임계값 로드 및 탐지 결과 생성 과정 (결과 status 대상 파일을 저장, status history 저장(파괴 및 생성), 개개별 파일에 history 생성 및 추가)
    #유저파일 정보 로드
    user_folder_list, threshold_list = load_userfile_info(user_folder)

    #이상치 탐지 및 이상치 결과 저장
    normal_user_ids = detection_and_savefile(total_df, threshold_list, day_output_path, history_output_path, current_date)

    #개별 유저 파일에 히스토리 추가 / 생성
    append_to_csv(total_df, user_folder)

    #test 데이터 train으로 이동
    move_files(Gyro_test_folder_path, Gyro_train_folder_path, normal_user_ids)