## 전처리 코드 모듈 + 임계값 설정 모듈

#라이브러리 로드
import os
import json
import datetime
import pandas as pd

# 전처리 함수
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

def append_to_csv(df, user_folder):
    pass

# 임계값 생성 함수
def determine_threshold(df, user_folder, user_file_list):
    pass


if __name__ == "__main__":
    ## 변수 및 path 준비과정
    # today filename 
    now = datetime.datetime.now()
    current_date = now.replace(year=2024).strftime('%Y%m%d')

    # load config
    base_path = os.getcwd()

    config_file_path = "train_config.json"
    with open('Train/' + config_file_path, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    # 각 폴더의 파일 리스트를 가져옵니다.
    Gyro_train_folder = os.listdir(config['input_json_file_path']["Gyro_train_data"])

    #통합 데이터 1차 저장경로
    total_filename = "Train_Total.csv"
    csv_output_path = (config['json_to_csv_path']['csv_file'] + total_filename)

    #유저별 데이터 폴더 생성 및 저장경로
    user_folder = config['user_file_path']['user_file'] 
    
    ## 전처리 과정
    #날짜별로 통합(데이터셋에 따라 맞춰서 구축)
    pass

    merged_df = json2csv(Gyro_train_folder)

    #통합 데이터 전처리
    total_df = preprocessing(merged_df)

    #통합 데이터 저장
    total_df.to_csv(csv_output_path)

    ##분류 및 임계값 생성 과정
    #ID별 폴더 생성 및 파일 생성
    user_file_list = append_to_csv(total_df, user_folder)

    #각 파일 위치에 이상치 저장
    determine_threshold(total_df, user_folder, user_file_list)


