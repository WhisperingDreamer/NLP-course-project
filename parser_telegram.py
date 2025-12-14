# Парсинг постов из Telegram

from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import asyncio
import csv

# импорт информации о парсере
from config import api_id, api_hash, phone

async def main():
    # для хранения спарсенных сообщений
    all_messages = [] 
    
    # К offset_id будет обращаться метод GetHistoryRequest для того, чтобы понять, с какого сообщения начать парсинг. Присваиваем ей значение 0, чтобы парсинг шёл с самого первого сообщения в канале. Если бы мы указали значение, равное 100, то первые 100 сообщений парсер бы пропустил.
    offset_id = 0
    
    # limit задаёт лимит на парсинг сообщений — за каждый цикл работы будет сохраняться только 100 сообщений
    limit = 100
    
    # выступает счётчиком спарсенных сообщений
    total_messages = 0
    
    # позволят нам задать ограничение на общее количество полученных сообщений. Если total_count_limit = 0, парсятся все сообщения
    total_count_limit = 3000
    
    # Авторизация и начало сессии запросов
    async with TelegramClient(phone, api_id, api_hash) as client:
        # указываем канал, это имя канала, например @morflot_gov
        channel = await client.get_entity('morflot_gov')
        channel2 = await client.get_entity('ohotnikizakorablyami')
        
        # Парсим сообщения из morflot_gov (15 585 подписчиков)
        # Получаем по 100 сообщений на каждой итерации
        while True:
            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                all_messages.append(message.message)
            offset_id = messages[len(messages) - 1].id
            total_messages += limit
            print("Спарсили " + str(total_messages) + " сообщений.")
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break
        
        #Cообщение для пользователя о том, что начался парсинг сообщений (запись в файл).
        print("Сохраняем данные в файл...") 
        
        with open("chats1.csv", "w", encoding="UTF-8", newline='') as f:
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            writer.writerow(["message"])
            for message in all_messages:
                writer.writerow([message])    
        
        #Сообщение об удачном парсинге чата.
        print("Парсинг сообщений группы morflot_gov успешно выполнен.")

    # для хранения спарсенных сообщений
    all_messages = [] 
    
    # К offset_id будет обращаться метод GetHistoryRequest для того, чтобы понять, с какого сообщения начать парсинг. Присваиваем ей значение 0, чтобы парсинг шёл с самого первого сообщения в канале. Если бы мы указали значение, равное 100, то первые 100 сообщений парсер бы пропустил.
    offset_id = 0
    
    # limit задаёт лимит на парсинг сообщений — за каждый цикл работы будет сохраняться только 100 сообщений
    limit = 100
    
    # выступает счётчиком спарсенных сообщений
    total_messages = 0
    
    # позволят нам задать ограничение на общее количество полученных сообщений. Если total_count_limit = 0, парсятся все сообщения
    total_count_limit = 10000

    # Авторизация и начало второй сессии запросов
    async with TelegramClient(phone, api_id, api_hash) as client:
        channel = await client.get_entity('ohotnikizakorablyami')
        
        # Парсим сообщения из ohotnikizakorablyami (2 775 подписчиков)
        # Получаем по 100 сообщений на каждой итерации
        while True:
            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            if not history.messages:
                break
            messages = history.messages
            for message in messages:
                all_messages.append(message.message)
            offset_id = messages[len(messages) - 1].id
            total_messages += limit
            print("Спарсили " + str(total_messages) + " сообщений.")
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break
        
        #Cообщение для пользователя о том, что начался парсинг сообщений (запись в файл).
        print("Сохраняем данные в файл...") 
        
        with open("chats2.csv", "w", encoding="UTF-8", newline='') as f:
            writer = csv.writer(f, delimiter=",", lineterminator="\n")
            writer.writerow(["message"])
            for message in all_messages:
                writer.writerow([message])    
        
        #Сообщение об удачном парсинге чата.
        print("Парсинг сообщений группы успешно выполнен.")
    
    
if __name__ == '__main__':
    asyncio.run(main())
