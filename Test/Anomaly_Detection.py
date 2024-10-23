## 전처리 코드 모듈 + 이상 탐지 코드 모듈

#라이브러리 로드
import os
import json
import datetime
import pandas as pd

#전처리 함수
def json2csv(file_path_list):
    df_list = []

    for file in file_path_list:
        with open(file, 'r') as f:
            data = json.load(f)
            # json 데이터를 pandas DataFrame으로 변환
            df = pd.DataFrame(data)
            df_list.append(df)


    # 모든 데이터프레임을 하나로 통합
    merged_df = pd.concat(df_list, ignore_index=True)

    return merged_df

def preprocessing(df):
    pass

def load_userfile_info(user_folder):
    pass

def move_files(source, destinations):
    pass

#이상탐지 함수
def detection(df, threshold_list, output_path):
    pass

def append_user_history(df, user_folder, user_file_list):
    pass

if __name__ == "__main__":
    ## 변수 및 path 준비과정
    # today filename 
    now = datetime.datetime.now()
    current_date = now.replace(year=2024).strftime('%Y%m%d')

    # load config
    base_path = os.getcwd()

    config_file_path = "test_config.json"
    with open('Test/' + config_file_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    # 각 폴더의 파일 리스트를 가져옵니다.
    Gyro_train_folder = os.listdir(config['input_json_file_path']["Gyro_test_data"])  

    #통합 데이터 1차 저장경로
    total_filename = "Test_Total.csv"
    csv_output_path = (config['json_to_csv_path']['csv_file'] + total_filename)      

    #유저별 데이터 접근 및 로드 경로
    user_folder = config['user_file_path']['user_file']

    #결과 저장 경로
    output_path = config['output_path']['output_result']

    #파일 이동 경로
    test_data_source = config['device_meter_folder_path']['device_test_path']
    train_data_destination =  config['device_meter_folder_path']['meter_train_path']


    ##전처리 과정
    #날짜별로 통합(데이터셋에 따라 맞춰서 구축)
    pass

    merged_df = json2csv(Gyro_train_folder)

    #통합 데이터 전처리
    total_df = preprocessing(merged_df)

    #통합 데이터 저장
    total_df.to_csv(csv_output_path)

    ##임계값 로드 및 탐지 결과 생성 과정 (결과 status 대상 파일을 저장, status history 저장(파괴 및 생성), 개개별 파일에 history 생성 및 추가)
    #유저파일 정보 로드
    user_file_list, threshold_list = load_userfile_info(user_folder)

    #이상치 탐지 및 이상치 결과 저장
    result_df = detection(total_df, threshold_list, output_path)

    #개별 유저 파일에 히스토리 추가 / 생성
    append_user_history(total_df, user_folder, user_file_list)

    #test 데이터 train으로 이동
    move_files(test_data_source, train_data_destination)
