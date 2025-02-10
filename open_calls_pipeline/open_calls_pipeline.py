import os
import csv
import pandas as pd
import requests
import openai
from bs4 import BeautifulSoup
from google.colab import drive

drive.mount('/content/drive', force_remount=True)

# Настройка API-ключа OpenAI
openai.api_key = 'KEY'

# Пути к файлам
DATA_DIR = '/content/drive/MyDrive/open_calls_ready_2'
OUTPUT_FILE = os.path.join(DATA_DIR, 'results.csv')
DB_UPLOAD_FILE = os.path.join(DATA_DIR, 'open_calls_after_25_11.csv')


# 1. Парсинг сайтов
def parse_open_calls():
    url = "https://www.artrabbit.com/artist-opportunities/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.content
    except requests.RequestException as e:
        print(f"Ошибка при получении страницы: {e}")
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    artopp_elements = soup.find_all('div', class_='artopp')

    data = []
    for artopp_element in artopp_elements:
        row = {
            "Data-d": artopp_element.get('data-d', ''),
            "Data-a": artopp_element.get('data-a', ''),
            "Heading": artopp_element.find('h3',
                                           class_='b_categorical-heading mod--artopps').text.strip() if artopp_element.find(
                'h3', class_='b_categorical-heading mod--artopps') else '',
            "Alert": artopp_element.find('p',
                                         class_='b_ending-alert mod--just-opened').text.strip() if artopp_element.find(
                'p', class_='b_ending-alert mod--just-opened') else '',
            "Title": artopp_element.find('h2').text.strip() if artopp_element.find('h2') else '',
            "Date Updated": artopp_element.find('p', class_='b_date').text.strip() if artopp_element.find('p',
                                                                                                          class_='b_date') else '',
            "Body": artopp_element.find('div', class_='m_body-copy').text.strip() if artopp_element.find('div',
                                                                                                         class_='m_body-copy') else '',
            "URL": artopp_element.find('a', class_='b_submit mod--next')['href'] if artopp_element.find('a',
                                                                                                        class_='b_submit mod--next') else ''
        }
        data.append(row)
    return data


# 2. Обработка данных через OpenAI
def ask_openai(question):
    try:
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": "You are a helpful assistant."},
                      {"role": "user", "content": question}],
            max_tokens=4000,
            temperature=1
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Ошибка при запросе к OpenAI: {e}")
        return "Error"


def process_csv_files():
    all_data = []
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.csv'):
            file_path = os.path.join(DATA_DIR, file_name)
            try:
                df = pd.read_csv(file_path)
                print(f"Файл {file_name} загружен")
                for _, row in df.iterrows():
                    data_str = " ".join([f"{col}: {row[col]}" for col in df.columns if pd.notna(row[col])])
                    all_data.append({
                        "City_Country": ask_openai(f"Extract only country name: {data_str}"),
                        "Open_Call_Title": ask_openai(f"Extract only open call title: {data_str}"),
                        "Deadline_Date": ask_openai(f"Extract only deadline date in YYYY-MM-DD: {data_str}"),
                        "Event_Date": ask_openai(f"Extract only event date in YYYY-MM-DD: {data_str}"),
                        "Application_Form_Link": ask_openai(f"Extract only application link: {data_str}"),
                        "Selection_Criteria": ask_openai(f"Extract only selection criteria: {data_str}"),
                        "FAQ": ask_openai(f"Generate FAQ from data: {data_str}"),
                        "Application_Guide": ask_openai(f"Generate application guide from data: {data_str}"),
                        "Fee": ask_openai(f"Extract only fee information: {data_str}")
                    })
            except Exception as e:
                print(f"Ошибка обработки файла {file_name}: {e}")
    return all_data


def save_results(results):
    df = pd.DataFrame(results)
    df.drop_duplicates(inplace=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    print(f"Результаты сохранены в {OUTPUT_FILE}")


# 3. Отправка данных в БД
def send_post_request(row):
    url = "https://beta.mirr.art/api/open_calls/"
    headers = {"Authorization": "Bearer ed1beebf3ede45c9a55835b5166c10b5", "Accept": "application/json"}
    data = {
        "city_country": row['City_Country'],
        "open_call_title": row['Open_Call_Title'],
        "deadline_date": row['Deadline_Date'],
        "event_date": row['Event_Date'],
        "application_from_link": row['Application_Form_Link'],
        "selection_criteria": row['Selection_Criteria'],
        "faq": row['FAQ'],
        "fee": row['Fee'],
        "application_guide": row['Application_Guide'],
        "open_call_description": f"Open call in {row['City_Country']} titled {row['Open_Call_Title']}."
    }
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            print(f"Успешно отправлены данные для: {row['Open_Call_Title']}")
        else:
            print(f"Ошибка отправки {row['Open_Call_Title']}: {response.status_code}")
    except Exception as e:
        print(f"Ошибка при отправке данных: {e}")


def upload_to_db():
    try:
        df = pd.read_csv(DB_UPLOAD_FILE)
        df.drop_duplicates(inplace=True)
        for _, row in df.iterrows():
            send_post_request(row)
    except Exception as e:
        print(f"Ошибка загрузки в БД: {e}")


# Запуск пайплайна
def run_pipeline():
    print("Запуск парсинга...")
    parsed_data = parse_open_calls()
    if parsed_data:
        save_results(parsed_data)

    print("Обработка OpenAI...")
    processed_data = process_csv_files()
    if processed_data:
        save_results(processed_data)

    print("Загрузка в БД...")
    upload_to_db()


if __name__ == "__main__":
    run_pipeline()
