import os
import traceback
from datetime import datetime
from threading import Thread
import time
import pandas as pd
import telebot

from get_token import get_token

# PATH_TO_REPORT_FOLDER = "X:/Projects/City_Scrapper/Logs/Reports/"
PATH_TO_REPORT_FOLDER = "C:/apr/Logs/Reports/"
bot = telebot.TeleBot(get_token())


class TelegramBot:
    def __init__(self):
        self.start_threads()

    def start_threads(self):
        Thread(target=self.polling).start()
        Thread(target=self.timer).start()

    def polling(self):
        while True:
            try:
                bot.polling()
            except:
                print(traceback.format_exc())

    def timer(self):
        while True:
            if time.localtime().tm_hour == 9 and time.localtime().tm_min == 0:
                print_data(692188099)
            time.sleep(60)


@bot.message_handler(commands=['print'])
def get_chat_id(message):
    print_data(message.chat.id)


def print_data(chat_id):
    print(chat_id)
    print_line = get_data()
    bot.send_message(chat_id, print_line)
    with open("report.xlsx", 'rb') as file:
        bot.send_document(chat_id, file)


def get_data():
    print_line = f"Report from {datetime.now().strftime('%Y-%m-%d')}\n"
    all_data = [[], [], []]
    for index, folder in enumerate(["Errors", "Imgs", "Liters"]):
        files_list = os.listdir(PATH_TO_REPORT_FOLDER + folder)
        files_list.sort(reverse=True)
        good_files = get_good_files(files_list)

        all_data[index] = get_pd_data(good_files, folder)
        all_data[index] = pd.concat(all_data[index]) if len(all_data[index]) > 1 else all_data[index]
        all_data[index] = all_data[index].drop_duplicates()

        condition = ~all_data[0]['Ошибка?'].isna() if folder == "Errors" else None
        print_line += print_unchecked_counts(all_data, index, folder, additional_condition=condition)
        if condition is not None:
            print_line += print_unchecked_counts(all_data, index, "Zeros", additional_condition=~condition)

    with pd.ExcelWriter("report.xlsx") as writer:
        for index, sheet_name in enumerate(["Errors", "Imgs", "Liters"]):
            all_data[index].to_excel(writer, sheet_name=sheet_name, index=False)

    return print_line


def get_good_files(files_list):
    time_now = time.time()
    good_files = []

    for file in files_list:
        time_stamp = time.mktime(datetime.strptime(file.replace(".xlsx", ""), "%Y-%m-%d_%H-%M-%S").timetuple())
        if time_now - time_stamp < 86400:  # Adjusted the condition to filter files within the last 24 hours
            good_files.append(file)
    return good_files


def get_pd_data(good_files, folder):
    all_data = []
    for file in good_files:
        data = pd.read_excel(PATH_TO_REPORT_FOLDER + folder + "/" + file)
        all_data.append(data)
    return all_data


def print_unchecked_counts(data, index, label, additional_condition=None):
    condition = data[index]['Проверено'] == False
    if additional_condition is not None:
        condition &= additional_condition
    return f"{label}: {data[index][condition].shape[0]}\n"


if __name__ == '__main__':
    TelegramBot()
