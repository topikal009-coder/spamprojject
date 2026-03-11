import asyncio
import os
import json
import re
import sys
import signal
from datetime import datetime, timedelta
from pyrogram import Client, filters, enums
from pyrogram.types import ReplyKeyboardMarkup
from pyrogram.errors import (
    PeerIdInvalid, Forbidden, SessionRevoked, 
    AuthKeyUnregistered, Unauthorized, FloodWait,
    ApiIdInvalid, AccessTokenInvalid
)
from pyrogram.handlers import DisconnectHandler
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- КОНФИГ ---
API_ID = 30032542
API_HASH = "ce646da1307fb452305d49f9bb8751ca"
BOT_TOKEN = os.environ.get('BOT_TOKEN', '8683043918:AAFd7vbh_2ROhCwcE2GcgIjg0XNyWwPL2kw')

# === КРИТИЧЕСКИ ВАЖНО: ПРАВИЛЬНАЯ РАБОЧАЯ ДИРЕКТОРИЯ ДЛЯ RAILWAY ===
# Определяем, где мы находимся
IS_RAILWAY = os.path.exists('/app') or 'RAILWAY_SERVICE_NAME' in os.environ

if IS_RAILWAY:
    # На Railway используем Volume, который должен быть смонтирован в /app/data
    WORK_DIR = '/app/data'
    # Проверяем, доступен ли Volume
    if not os.path.exists(WORK_DIR):
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: Volume не смонтирован в {WORK_DIR}")
        logger.error("Создайте Volume в Railway и смонтируйте его в /app/data")
        # Пробуем создать, но данные не сохранятся при перезапуске
        os.makedirs(WORK_DIR, exist_ok=True)
else:
    # Локально используем текущую папку
    WORK_DIR = os.path.dirname(os.path.abspath(__file__))

# Создаем все необходимые папки
os.makedirs(WORK_DIR, exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'sessions'), exist_ok=True)
os.makedirs(os.path.join(WORK_DIR, 'user_settings'), exist_ok=True)

logger.info(f"📁 Рабочая директория: {WORK_DIR}")
logger.info(f"📁 Папка сессий: {os.path.join(WORK_DIR, 'sessions')}")
logger.info(f"📁 На Railway: {IS_RAILWAY}")

# === НАСТРОЙКА ОДНОРАЗОВЫХ КЛЮЧЕЙ ===
KEYS_FILE = os.path.join(WORK_DIR, 'activation_keys.json')

def load_keys():
    """Загружает ключи из файла"""
    default_keys = {
        "artem": "Пользователь 1",
        "pryma": "Пользователь 2",
        "igor": "Пользователь 3", 
        "fbfs-sdfs-456d-h34k": "Пользователь 4",
        "jhsd-j34k-dfyt-mh3l": "Пользователь 5", 
        "34gd-fgh5-hfg3-s37h": "Пользователь 6",
        "ADMINKEY999": "Администратор",
    }
    
    try:
        if os.path.exists(KEYS_FILE):
            with open(KEYS_FILE, 'r', encoding='utf-8') as f:
                keys = json.load(f)
                logger.info(f"✅ Загружено {len(keys)} ключей из {KEYS_FILE}")
                return keys
        else:
            # Создаем файл с ключами по умолчанию
            with open(KEYS_FILE, 'w', encoding='utf-8') as f:
                json.dump(default_keys, f, ensure_ascii=False, indent=2)
            logger.info(f"✅ Создан файл ключей: {KEYS_FILE}")
            return default_keys
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки ключей: {e}")
        return default_keys

def save_keys(keys):
    """Сохраняет ключи в файл"""
    try:
        with open(KEYS_FILE, 'w', encoding='utf-8') as f:
            json.dump(keys, f, ensure_ascii=False, indent=2)
        logger.info("✅ Ключи сохранены")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения ключей: {e}")
        return False

# Загружаем ключи
ONE_TIME_KEYS = load_keys()

KEY_EXPIRY_DAYS = 30
MAX_ACCOUNTS_PER_USER = 3
# ====================================

# === ВАЖНО: ПРАВИЛЬНАЯ ИНИЦИАЛИЗАЦИЯ КЛИЕНТОВ ===
# Для бота-менеджера используем отдельную папку
bot_session_dir = os.path.join(WORK_DIR, 'bot_session')
os.makedirs(bot_session_dir, exist_ok=True)

bot = Client(
    "manager_bot", 
    api_id=API_ID, 
    api_hash=API_HASH, 
    bot_token=BOT_TOKEN,
    workdir=bot_session_dir  # Важно! Указываем рабочую папку для сессии бота
)

# Структура данных пользователей
users_data = {}
temp_auth = {}
users_file = os.path.join(WORK_DIR, "bot_users.json")
reconnect_tasks = {}
keep_alive_tasks = {}

# --- ФУНКЦИИ ДЛЯ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ ---

def save_users():
    """Сохраняет данные пользователей в файл"""
    try:
        users_to_save = {}
        for uid, data in users_data.items():
            accounts = {}
            for phone, acc in data["accounts"].items():
                # Очищаем номер для имени файла
                clean_phone = phone.replace('+', '').replace(' ', '')
                session_path = os.path.join(WORK_DIR, 'sessions', f"{clean_phone}_{uid}")
                
                accounts[phone] = {
                    "text": acc["text"],
                    "interval": acc["interval"],
                    "running": False,  # Не сохраняем состояние запуска
                    "added_date": acc["added_date"].isoformat() if isinstance(acc["added_date"], datetime) else acc["added_date"],
                    "session_name": session_path
                }
            
            users_to_save[str(uid)] = {
                "expires": data["expires"].isoformat() if isinstance(data["expires"], datetime) else data["expires"],
                "key_used": data["key_used"],
                "is_admin": data["is_admin"],
                "username": data.get("username", ""),
                "bound_username": data.get("bound_username", ""),
                "accounts": accounts
            }
        
        with open(users_file, 'w', encoding='utf-8') as f:
            json.dump(users_to_save, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ Сохранено {len(users_data)} пользователей")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения пользователей: {e}")
        return False

def load_users():
    """Загружает данные пользователей из файла"""
    global users_data
    try:
        if os.path.exists(users_file):
            with open(users_file, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
            
            for uid, data in loaded_data.items():
                uid = int(uid)
                expires = data["expires"]
                if isinstance(expires, str):
                    expires = datetime.fromisoformat(expires)
                
                if expires > datetime.now():
                    accounts = {}
                    for phone, acc_data in data.get("accounts", {}).items():
                        accounts[phone] = {
                            "text": acc_data["text"],
                            "interval": acc_data["interval"],
                            "running": False,
                            "added_date": datetime.fromisoformat(acc_data["added_date"]) if isinstance(acc_data.get("added_date"), str) else datetime.now(),
                            "session_name": acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{uid}"))
                        }
                    
                    users_data[uid] = {
                        "expires": expires,
                        "key_used": data["key_used"],
                        "is_admin": data["is_admin"],
                        "username": data.get("username", ""),
                        "bound_username": data.get("bound_username", ""),
                        "accounts": accounts
                    }
            
            logger.info(f"✅ Загружено {len(users_data)} пользователей")
            return True
        else:
            logger.info("📭 Файл пользователей не найден, начинаем с пустой базой")
            return False
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки пользователей: {e}")
        return False

# === НОВАЯ ФУНКЦИЯ: Постоянная проверка соединения ===
async def keep_alive(user_id, phone, client):
    """Держит соединение активным"""
    key = f"{user_id}_{phone}"
    
    while True:
        try:
            # Проверяем, существует ли еще клиент
            if key not in keep_alive_tasks:
                break
                
            # Проверяем соединение
            await asyncio.wait_for(client.get_me(), timeout=10)
            logger.debug(f"💓 Keep-alive для {phone}")
            
            # Ждем 30 секунд перед следующей проверкой
            await asyncio.sleep(30)
            
        except asyncio.CancelledError:
            logger.info(f"🛑 Keep-alive остановлен для {phone}")
            break
        except Exception as e:
            logger.warning(f"⚠️ Keep-alive ошибка для {phone}: {e}")
            
            # Пробуем переподключиться
            if key in keep_alive_tasks:
                await schedule_reconnect(user_id, phone)
            break

def parse_key_with_username(key_text):
    """Парсит ключ в формате 'key123-@username'"""
    pattern = r'^(.*?)-@([a-zA-Z0-9_]+)$'
    match = re.match(pattern, key_text.strip())
    
    if match:
        key = match.group(1)
        username = match.group(2)
        return key, username
    else:
        return key_text.strip(), None

def check_key_binding(key, user_id, username):
    """Проверяет привязку ключа к пользователю"""
    current_keys = load_keys()
    
    if key not in current_keys:
        return False, "Ключ не существует"
    
    key_value = current_keys[key]
    
    if isinstance(key_value, str) and key_value.startswith('@'):
        bound_username = key_value.replace('@', '')
        
        user_clean = username.replace('@', '') if username else ''
        
        if user_clean.lower() != bound_username.lower():
            return False, f"❌ Этот ключ привязан к пользователю @{bound_username}"
        else:
            return True, "Ключ подходит"
    else:
        return True, "Обычный ключ"

async def load_user_sessions():
    """Загружает сессии для всех пользователей"""
    sessions_dir = os.path.join(WORK_DIR, 'sessions')
    
    if not os.path.exists(sessions_dir):
        os.makedirs(sessions_dir)
        logger.info(f"📁 Создана папка сессий: {sessions_dir}")
    
    loaded_count = 0
    for user_id, user_data in users_data.items():
        for phone, acc_data in user_data["accounts"].items():
            try:
                session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
                
                # Проверяем существование файла сессии
                session_file = f"{session_name}.session"
                if os.path.exists(session_file):
                    logger.info(f"📄 Найден файл сессии: {session_file}")
                    
                    client = Client(
                        session_name, 
                        api_id=API_ID, 
                        api_hash=API_HASH,
                        workdir=WORK_DIR  # Важно! Указываем рабочую папку
                    )
                    
                    # Добавляем обработчик отключения
                    async def on_disconnect(client, user_id=user_id, phone=phone):
                        logger.warning(f"⚠️ Аккаунт {phone} отключился")
                        await schedule_reconnect(user_id, phone)
                    
                    client.add_handler(DisconnectHandler(on_disconnect))
                    
                    try:
                        await client.start()
                        acc_data["client"] = client
                        
                        # Запускаем keep-alive для этого клиента
                        task_key = f"{user_id}_{phone}"
                        if task_key in keep_alive_tasks:
                            keep_alive_tasks[task_key].cancel()
                        keep_alive_tasks[task_key] = asyncio.create_task(
                            keep_alive(user_id, phone, client)
                        )
                        
                        loaded_count += 1
                        logger.info(f"✅ Сессия {phone} загружена для пользователя {user_id}")
                        
                    except (SessionRevoked, AuthKeyUnregistered, Unauthorized) as e:
                        logger.error(f"❌ Сессия {phone} недействительна: {e}")
                        if os.path.exists(session_file):
                            os.remove(session_file)
                            logger.info(f"🗑 Удален недействительный файл сессии: {session_file}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
                else:
                    logger.warning(f"⚠️ Файл сессии {session_file} не найден")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки сессии {phone}: {e}")
    
    logger.info(f"✅ Загружено {loaded_count} активных сессий")
    return loaded_count

async def schedule_reconnect(user_id, phone):
    """Планирует переподключение аккаунта"""
    key = f"{user_id}_{phone}"
    
    # Отменяем существующую задачу переподключения
    if key in reconnect_tasks:
        reconnect_tasks[key].cancel()
    
    # Отменяем keep-alive
    if key in keep_alive_tasks:
        keep_alive_tasks[key].cancel()
    
    # Создаем новую задачу с задержкой
    async def reconnect_with_delay():
        await asyncio.sleep(30)  # Уменьшил до 30 секунд для более быстрого переподключения
        try:
            await reconnect_account(user_id, phone)
        except Exception as e:
            logger.error(f"❌ Ошибка переподключения {phone}: {e}")
    
    reconnect_tasks[key] = asyncio.create_task(reconnect_with_delay())
    logger.info(f"⏰ Запланировано переподключение {phone} через 30 сек")

async def reconnect_account(user_id, phone):
    """Переподключает аккаунт"""
    if user_id not in users_data or phone not in users_data[user_id]["accounts"]:
        return
    
    acc_data = users_data[user_id]["accounts"][phone]
    session_name = acc_data.get("session_name", os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}"))
    
    try:
        logger.info(f"🔄 Попытка переподключения {phone}")
        
        # Пробуем переподключиться
        client = Client(
            session_name, 
            api_id=API_ID, 
            api_hash=API_HASH,
            workdir=WORK_DIR
        )
        
        async def on_disconnect(client, user_id=user_id, phone=phone):
            logger.warning(f"⚠️ Аккаунт {phone} снова отключился")
            await schedule_reconnect(user_id, phone)
        
        client.add_handler(DisconnectHandler(on_disconnect))
        await client.start()
        
        # Восстанавливаем состояние
        acc_data["client"] = client
        
        # Запускаем keep-alive
        key = f"{user_id}_{phone}"
        if key in keep_alive_tasks:
            keep_alive_tasks[key].cancel()
        keep_alive_tasks[key] = asyncio.create_task(
            keep_alive(user_id, phone, client)
        )
        
        if acc_data.get("running", False):
            # Если рассылка была активна, перезапускаем
            asyncio.create_task(spam_cycle(user_id, phone, acc_data, None))
        
        logger.info(f"✅ Аккаунт {phone} успешно переподключен")
        
    except Exception as e:
        logger.error(f"❌ Не удалось переподключить {phone}: {e}")
        
        # Пробуем еще раз через минуту
        key = f"{user_id}_{phone}"
        if key not in reconnect_tasks:
            await schedule_reconnect(user_id, phone)

def check_access(user_id):
    """Проверяет доступ пользователя"""
    if user_id in users_data:
        user_data = users_data[user_id]
        expires = user_data["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        if expires > datetime.now():
            return True
        else:
            # Закрываем все клиенты перед удалением
            for acc in user_data["accounts"].values():
                if "client" in acc:
                    try:
                        asyncio.create_task(acc["client"].stop())
                    except:
                        pass
            del users_data[user_id]
            save_users()
    return False

def is_admin(user_id):
    """Проверяет, является ли пользователь администратором"""
    if user_id in users_data:
        return users_data[user_id].get("is_admin", False)
    return False

def get_user_main_keyboard(user_id):
    """Возвращает клавиатуру для конкретного пользователя"""
    if is_admin(user_id):
        return ReplyKeyboardMarkup([
            ["➕ Добавить аккаунт", "📱 Мои аккаунты"],
            ["👤 Мой кабинет", "🚀 Старт рассылки"],
            ["🛑 Стоп рассылки", "⚙️ Настройки текста"],
            ["⏱ Настройки интервала", "💾 Сохранить настройки"],
            ["📂 Загрузить настройки", "🔑 Управление ключами"],
            ["👥 Все пользователи", "📊 Статистика"],
            ["🔗 Привязать ключ к юзеру"]
        ], resize_keyboard=True)
    else:
        return ReplyKeyboardMarkup([
            ["➕ Добавить аккаунт", "📱 Мои аккаунты"],
            ["👤 Мой кабинет", "🚀 Старт рассылки"],
            ["🛑 Стоп рассылки", "⚙️ Настройки текста"],
            ["⏱ Настройки интервала", "💾 Сохранить настройки"],
            ["📂 Загрузить настройки", "🔑 Информация о доступе"]
        ], resize_keyboard=True)

# --- ФУНКЦИИ ДЛЯ РАССЫЛКИ ---

async def spam_cycle(user_id, phone, data, message):
    """Фоновый процесс рассылки для конкретного пользователя"""
    status_msg = None
    if message:
        status_msg = await message.reply(f"🚀 Запуск рассылки для {phone}...")
    
    sent_chats = []
    error_count = 0
    cycle_count = 0

    while data.get("running", False):
        try:
            if "client" not in data:
                logger.error(f"❌ Нет клиента для {phone}")
                error_count += 1
                if error_count > 3:
                    break
                await asyncio.sleep(60)
                continue
            
            # Проверяем, подключен ли клиент
            try:
                me = await data["client"].get_me()
                if not me:
                    raise Exception("Не удалось получить информацию о пользователе")
            except Exception as e:
                logger.warning(f"⚠️ Клиент {phone} не отвечает: {e}")
                await reconnect_account(user_id, phone)
                await asyncio.sleep(30)
                continue
            
            # Собираем чаты для рассылки
            dialogs = []
            async for dialog in data["client"].get_dialogs():
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    dialogs.append(dialog)
            
            # Отправляем сообщения
            for dialog in dialogs:
                if not data.get("running", False): 
                    break
                
                try:
                    await data["client"].send_message(dialog.chat.id, data["text"])
                    sent_chats.append(dialog.chat.title)
                    
                    # Обновляем статус каждые 5 отправок
                    if len(sent_chats) % 5 == 0 and status_msg:
                        new_text = f"🚀 Рассылка {phone} активна\n\n"
                        new_text += f"📊 Цикл #{cycle_count + 1}\n"
                        new_text += f"📨 Отправлено в {len(sent_chats)} чатов\n"
                        new_text += f"📝 Последние 5:\n" + "\n".join(sent_chats[-5:])
                        try:
                            await status_msg.edit_text(new_text)
                        except:
                            pass
                    
                    await asyncio.sleep(0.5)  # Небольшая задержка между сообщениями
                    
                except FloodWait as e:
                    wait_time = e.value
                    logger.warning(f"⚠️ FloodWait на {wait_time} секунд для {phone}")
                    await asyncio.sleep(wait_time)
                except (PeerIdInvalid, Forbidden): 
                    continue
                except Exception as e:
                    logger.error(f"Ошибка отправки в {dialog.chat.title}: {e}")
                    continue
            
            cycle_count += 1
            error_count = 0
            
            # Ждем перед следующим циклом
            wait_time = data["interval"]
            logger.info(f"⏱ Цикл {cycle_count} для {phone} завершен. Следующий через {wait_time} сек")
            
            # Постепенный сон с проверкой статуса
            for _ in range(wait_time):
                if not data.get("running", False):
                    break
                await asyncio.sleep(1)
            
        except Exception as e:
            logger.error(f"Ошибка в цикле рассылки {phone}: {e}")
            error_count += 1
            if error_count > 5:
                logger.error(f"❌ Слишком много ошибок для {phone}, останавливаем рассылку")
                data["running"] = False
                break
            await asyncio.sleep(60)
   
    if status_msg:
        try:
            await status_msg.edit_text(
                f"✅ Рассылка {phone} завершена.\n"
                f"📊 Всего циклов: {cycle_count}\n"
                f"📨 Всего чатов: {len(sent_chats)}"
            )
        except:
            pass
    
    logger.info(f"✅ Рассылка {phone} остановлена. Отправлено в {len(sent_chats)} чатов за {cycle_count} циклов")

# --- ХЕНДЛЕРЫ ---

@bot.on_message(filters.command("start"))
async def start(c, m):
    user_id = m.from_user.id
    username = m.from_user.username or m.from_user.first_name
    
    logger.info(f"Пользователь {user_id} (@{m.from_user.username}) запустил /start")
    
    if check_access(user_id):
        accounts_count = len(users_data[user_id]["accounts"])
        expires = users_data[user_id]["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        
        bound_info = ""
        if users_data[user_id].get("bound_username"):
            bound_info = f"🔗 Привязан к: @{users_data[user_id]['bound_username']}\n"
        
        await m.reply(
            f"👋 Добро пожаловать в личный кабинет, {username}!\n\n"
            f"📊 Ваша статистика:\n"
            f"📱 Аккаунтов: {accounts_count}/{MAX_ACCOUNTS_PER_USER}\n"
            f"📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n"
            f"{bound_info}"
            f"👑 Статус: {'Администратор' if is_admin(user_id) else 'Пользователь'}",
            reply_markup=get_user_main_keyboard(user_id)
        )
    else:
        await m.reply(
            "🔐 Доступ ограничен\n\n"
            "Для использования бота введите одноразовый ключ доступа.\n"
            "Ключ можно ввести в формате:\n"
            "• обычный ключ: KEY123\n"
            "• привязанный ключ: KEY123-@username\n\n"
            "Нажмите кнопку ниже чтобы ввести ключ.",
            reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
        )

@bot.on_message(filters.regex("🔑 Ввести ключ доступа"))
async def enter_key_prompt(c, m):
    user_id = m.from_user.id
    logger.info(f"Пользователь {user_id} нажал кнопку ввода ключа")
    
    if check_access(user_id):
        return await m.reply(
            "✅ У вас уже есть активный доступ!\n"
            "Используйте /start для входа в личный кабинет.",
            reply_markup=get_user_main_keyboard(user_id)
        )
    
    temp_auth[user_id] = {"step": "enter_key", "user_id": user_id}
    logger.info(f"Установлен шаг enter_key для пользователя {user_id}")
    
    await m.reply(
        "🔑 Пожалуйста, введите ваш одноразовый ключ доступа:\n\n"
        "Форматы:\n"
        "• KEY123 - обычный ключ\n"
        "• KEY123-@username - ключ для конкретного пользователя",
        reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
    )

@bot.on_message(filters.regex("🔙 Отмена"))
async def cancel_input(c, m):
    user_id = m.from_user.id
    logger.info(f"Пользователь {user_id} отменил ввод")
    
    if user_id in temp_auth:
        temp_auth.pop(user_id)
    
    await m.reply(
        "❌ Ввод отменен.\n"
        "Используйте /start для возврата в главное меню.",
        reply_markup=ReplyKeyboardMarkup([["🔑 Ввести ключ доступа"]], resize_keyboard=True)
    )

@bot.on_message(filters.text & filters.private)
async def handle_all_messages(c, m):
    """Универсальный обработчик всех текстовых сообщений"""
    user_id = m.from_user.id
    text = m.text
    
    logger.info(f"Получено сообщение от {user_id}: {text}")
    
    # Проверяем, находится ли пользователь в режиме ввода
    if user_id in temp_auth:
        step = temp_auth[user_id].get("step")
        logger.info(f"Пользователь {user_id} в шаге: {step}")
        
        if step == "enter_key":
            await handle_key_input(c, m)
            return
        elif step == "phone":
            await handle_phone_input(c, m)
            return
        elif step == "code":
            await handle_code_input(c, m)
            return
        elif step == "password":
            await handle_password_input(c, m)
            return
        elif step == "text":
            await handle_text_input(c, m)
            return
        elif step == "interval":
            await handle_interval_input(c, m)
            return
        elif step == "confirm_interval":
            await handle_interval_confirm(c, m)
            return
        elif step == "bind_key":
            await handle_bind_key(c, m)
            return
    
    # Если пользователь не в режиме ввода, проверяем команды из меню
    await handle_menu_commands(c, m)

async def handle_key_input(c, m):
    """Обработка ввода ключа доступа"""
    user_id = m.from_user.id
    raw_key = m.text.strip()
    username = m.from_user.username or ""
    
    logger.info(f"Пользователь {user_id} ввел ключ: {raw_key}")
    
    # Парсим ключ и username
    key, bound_username = parse_key_with_username(raw_key)
    
    # Загружаем актуальные ключи
    current_keys = load_keys()
    
    if key in current_keys:
        # Проверяем привязку
        can_use, message = check_key_binding(key, user_id, username)
        
        if not can_use:
            await m.reply(message)
            logger.info(f"❌ Ключ {key} отклонен для {user_id}: {message}")
            return
        
        # Проверяем, не использован ли ключ
        key_used = False
        for uid, user_data in users_data.items():
            if user_data["key_used"] == key:
                key_used = True
                if uid == user_id:
                    await m.reply("❌ Вы уже использовали этот ключ!")
                    return
                break
        
        if key_used:
            await m.reply("❌ Этот ключ уже был использован другим пользователем!")
            logger.info(f"Ключ {key} уже использован")
        else:
            owner = current_keys[key]
            
            # Определяем, является ли ключ админским
            is_admin_key = "ADMIN" in key or "админ" in owner.lower() or key == "ADMINKEY999"
            
            expires = datetime.now() + timedelta(days=KEY_EXPIRY_DAYS)
            
            # Создаем нового пользователя
            users_data[user_id] = {
                "expires": expires.isoformat(),
                "key_used": key,
                "is_admin": is_admin_key,
                "username": username or m.from_user.first_name,
                "bound_username": bound_username if bound_username else "",
                "accounts": {}
            }
            
            if save_users():
                role = "👑 Администратор" if is_admin_key else "👤 Пользователь"
                bound_info = f"🔗 Привязан к: @{bound_username}\n" if bound_username else ""
                
                await m.reply(
                    f"✅ Доступ предоставлен!\n\n"
                    f"{role}\n"
                    f"Ключ: {key}\n"
                    f"Владелец ключа: {owner}\n"
                    f"{bound_info}"
                    f"Срок действия до: {expires.strftime('%d.%m.%Y %H:%M')}\n\n"
                    f"Используйте /start для входа в личный кабинет",
                    reply_markup=get_user_main_keyboard(user_id)
                )
                
                logger.info(f"✅ Пользователь {user_id} получил доступ с ключом {key}")
            else:
                await m.reply("❌ Ошибка при сохранении данных. Попробуйте позже.")
            
            # Очищаем временные данные
            if user_id in temp_auth:
                temp_auth.pop(user_id)
    else:
        await m.reply("❌ Неверный ключ доступа!")
        logger.info(f"Неверный ключ: {key}")

async def handle_bind_key(c, m):
    """Обработка привязки ключа к пользователю"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    text = m.text.strip()
    
    # Проверяем формат
    key, username = parse_key_with_username(text)
    
    if not username:
        await m.reply("❌ Неверный формат! Используйте: ключ-@username\nНапример: KEY123-@durov")
        return
    
    # Загружаем текущие ключи
    current_keys = load_keys()
    
    # Добавляем или обновляем ключ
    current_keys[key] = f"@{username}"
    
    if save_keys(current_keys):
        await m.reply(
            f"✅ Ключ успешно привязан!\n\n"
            f"🔑 Ключ: {key}\n"
            f"👤 Привязан к: @{username}\n\n"
            f"Теперь этот ключ может использовать только пользователь @{username}"
        )
    else:
        await m.reply("❌ Ошибка при сохранении ключа")
    
    temp_auth.pop(user_id)

async def handle_phone_input(c, m):
    """Обработка ввода номера телефона"""
    user_id = m.from_user.id
    phone = m.text
    
    try:
        session_name = os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}")
        logger.info(f"📱 Создание сессии для {phone} в {session_name}")
        
        client = Client(
            session_name, 
            api_id=API_ID, 
            api_hash=API_HASH, 
            phone_number=phone,
            workdir=WORK_DIR
        )
        
        await client.connect()
        sent = await client.send_code(phone)
        
        temp_auth[user_id].update({
            "client": client,
            "phone": phone,
            "code_hash": sent.phone_code_hash,
            "step": "code"
        })
        await m.reply("🔢 Введите код из СМС:")
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        logger.error(f"Ошибка при добавлении телефона {phone}: {e}")
        temp_auth.pop(user_id, None)

async def handle_code_input(c, m):
    """Обработка ввода кода"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    try:
        await data["client"].sign_in(data["phone"], data["code_hash"], m.text)
        await finalize_user_account(user_id, data, m)
    except Exception as e:
        if "SESSION_PASSWORD_NEEDED" in str(e):
            data["step"] = "password"
            await m.reply("🔐 Введите облачный пароль (2FA):")
        else:
            await m.reply(f"❌ Ошибка: {e}")
            logger.error(f"Ошибка при вводе кода: {e}")
            temp_auth.pop(user_id, None)

async def handle_password_input(c, m):
    """Обработка ввода пароля 2FA"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    try:
        await data["client"].check_password(m.text)
        await finalize_user_account(user_id, data, m)
    except Exception as e:
        await m.reply(f"❌ Ошибка: {e}")
        logger.error(f"Ошибка при вводе пароля: {e}")
        temp_auth.pop(user_id, None)

async def handle_text_input(c, m):
    """Обработка ввода текста рассылки"""
    user_id = m.from_user.id
    
    if user_id not in users_data:
        await m.reply("❌ Пользователь не найден")
        temp_auth.pop(user_id, None)
        return
    
    for acc in users_data[user_id]["accounts"].values():
        acc["text"] = m.text
    
    save_users()
    await m.reply("✅ Текст рассылки обновлен для всех ваших аккаунтов.")
    temp_auth.pop(user_id)

async def handle_interval_input(c, m):
    """Обработка ввода интервала"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    try:
        interval = int(m.text)
        if interval < 10:
            await m.reply("⚠️ Интервал меньше 10 секунд может привести к бану. Продолжить? (да/нет)")
            data["step"] = "confirm_interval"
            data["temp_interval"] = interval
        else:
            for acc in users_data[user_id]["accounts"].values():
                acc["interval"] = interval
            save_users()
            await m.reply(f"✅ Интервал установлен: {interval} сек.")
            temp_auth.pop(user_id)
    except ValueError:
        await m.reply("❌ Пожалуйста, введите число!")

async def handle_interval_confirm(c, m):
    """Подтверждение интервала"""
    user_id = m.from_user.id
    data = temp_auth[user_id]
    
    if m.text.lower() in ["да", "yes", "д", "y"]:
        for acc in users_data[user_id]["accounts"].values():
            acc["interval"] = data["temp_interval"]
        save_users()
        await m.reply(f"✅ Интервал установлен: {data['temp_interval']} сек. (Будьте осторожны!)")
    else:
        await m.reply("❌ Установка интервала отменена.")
    
    temp_auth.pop(user_id)

async def handle_menu_commands(c, m):
    """Обработка команд из меню"""
    user_id = m.from_user.id
    text = m.text
    
    # Сначала проверяем доступ
    if not check_access(user_id):
        await m.reply("❌ У вас нет доступа. Используйте /start для входа.")
        return
    
    # Обрабатываем команды меню
    if text == "➕ Добавить аккаунт":
        if len(users_data[user_id]["accounts"]) >= MAX_ACCOUNTS_PER_USER:
            await m.reply(f"❌ Вы достигли лимита аккаунтов ({MAX_ACCOUNTS_PER_USER}).")
        else:
            temp_auth[user_id] = {"step": "phone", "user_id": user_id}
            await m.reply("📱 Введите номер телефона в международном формате (например, +380123456789):")
    
    elif text == "📱 Мои аккаунты":
        accounts = users_data[user_id]["accounts"]
        if not accounts:
            await m.reply("📱 У вас нет добавленных аккаунтов.")
        else:
            acc_list = []
            for i, (phone, data) in enumerate(accounts.items(), 1):
                status = "🟢 АКТИВЕН" if data.get("running", False) else "🔴 ОСТАНОВЛЕН"
                client_status = "✅" if "client" in data else "❌"
                acc_list.append(
                    f"{i}. {phone}\n"
                    f"   Статус: {status} | Клиент: {client_status}\n"
                    f"   📝 Текст: {data['text'][:30]}...\n"
                    f"   ⏱ Интервал: {data['interval']} сек."
                )
            await m.reply("📱 Ваши аккаунты:\n\n" + "\n\n".join(acc_list))
    
    elif text == "👤 Мой кабинет":
        user_data = users_data[user_id]
        accounts = user_data["accounts"]
        
        expires = user_data["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        
        total_accounts = len(accounts)
        running_accounts = sum(1 for acc in accounts.values() if acc.get("running", False))
        active_clients = sum(1 for acc in accounts.values() if "client" in acc)
        
        bound_info = f"🔗 Привязан к: @{user_data['bound_username']}\n" if user_data.get('bound_username') else ""
        
        accounts_info = ""
        for phone, acc in accounts.items():
            status = "🟢" if acc.get("running", False) else "🔴"
            client = "✅" if "client" in acc else "❌"
            accounts_info += f"{status}{client} {phone}\n   📝 {acc['text'][:20]}...\n"
        
        await m.reply(
            f"👤 Личный кабинет\n\n"
            f"🆔 ID: {user_id}\n"
            f"👤 Имя: {user_data.get('username', 'Не указано')}\n"
            f"{bound_info}"
            f"📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n"
            f"🔑 Использован ключ: {user_data['key_used']}\n"
            f"👑 Админ: {'Да' if is_admin(user_id) else 'Нет'}\n\n"
            f"📊 Статистика аккаунтов:\n"
            f"📱 Всего: {total_accounts}/{MAX_ACCOUNTS_PER_USER}\n"
            f"✅ Активных клиентов: {active_clients}\n"
            f"🟢 Активных рассылок: {running_accounts}\n\n"
            f"📋 Ваши аккаунты:\n{accounts_info}"
        )
    
    elif text == "🚀 Старт рассылки":
        accounts = users_data[user_id]["accounts"]
        if not accounts:
            await m.reply("❌ У вас нет добавленных аккаунтов!")
        else:
            started = 0
            for phone, d in accounts.items():
                if not d.get("running", False):
                    if "client" not in d:
                        await reconnect_account(user_id, phone)
                        await asyncio.sleep(2)
                    
                    if "client" in d:
                        d["running"] = True
                        asyncio.create_task(spam_cycle(user_id, phone, d, m))
                        started += 1
            
            await m.reply(f"🚀 Запущено рассылок: {started}")
    
    elif text == "🛑 Стоп рассылки":
        accounts = users_data[user_id]["accounts"]
        stopped = 0
        for d in accounts.values():
            if d.get("running", False):
                d["running"] = False
                stopped += 1
        
        save_users()
        await m.reply(f"🛑 Остановлено рассылок: {stopped}")
    
    elif text == "⚙️ Настройки текста":
        if not users_data[user_id]["accounts"]:
            await m.reply("❌ Сначала добавьте аккаунт!")
        else:
            temp_auth[user_id] = {"step": "text", "user_id": user_id}
            await m.reply("✏️ Введите новый текст для рассылки:")
    
    elif text == "⏱ Настройки интервала":
        if not users_data[user_id]["accounts"]:
            await m.reply("❌ Сначала добавьте аккаунт!")
        else:
            temp_auth[user_id] = {"step": "interval", "user_id": user_id}
            await m.reply("⏱ Введите интервал между циклами рассылки (в секундах):")
    
    elif text == "🔑 Информация о доступе":
        data = users_data[user_id]
        expires = data["expires"]
        if isinstance(expires, str):
            expires = datetime.fromisoformat(expires)
        
        days_left = (expires - datetime.now()).days
        
        bound_info = f"🔗 Привязан к: @{data['bound_username']}\n" if data.get('bound_username') else ""
        
        await m.reply(
            f"🔑 Информация о доступе:\n\n"
            f"✅ Доступ активен\n"
            f"🔑 Ключ: {data['key_used']}\n"
            f"{bound_info}"
            f"📅 Истекает: {expires.strftime('%d.%m.%Y')}\n"
            f"⏳ Осталось дней: {days_left}\n"
            f"👑 Права: {'Администратор' if is_admin(user_id) else 'Пользователь'}"
        )
    
    elif text == "💾 Сохранить настройки":
        if save_users():
            await m.reply("✅ Настройки сохранены")
        else:
            await m.reply("❌ Ошибка при сохранении")
    
    elif text == "📂 Загрузить настройки":
        if load_users():
            await m.reply("✅ Настройки загружены")
        else:
            await m.reply("❌ Ошибка при загрузке")
    
    # Админские команды
    elif is_admin(user_id):
        if text == "🔑 Управление ключами":
            current_keys = load_keys()
            keys_list = "📋 Доступные одноразовые ключи:\n\n"
            for key, owner in current_keys.items():
                used = False
                used_by = ""
                bound_to = ""
                
                if isinstance(owner, str) and owner.startswith('@'):
                    bound_to = f" (привязан к {owner})"
                    display_key = key
                else:
                    display_key = key
                
                for uid, user_data in users_data.items():
                    if user_data["key_used"] == key:
                        used = True
                        used_by = f" (использован: {user_data.get('username', uid)})"
                        break
                
                status = "❌" if used else "✅"
                keys_list += f"{status} {display_key} - {owner}{bound_to}{used_by}\n"
            
            keys_list += "\n\n📝 Команды:\n"
            keys_list += "• Обычный ключ: просто ключ\n"
            keys_list += "• Привязанный ключ: ключ-@username\n"
            keys_list += "Нажмите кнопку ниже для привязки ключа"
            
            await m.reply(
                keys_list,
                reply_markup=ReplyKeyboardMarkup([["🔗 Привязать ключ к юзеру", "🔙 Назад"]], resize_keyboard=True)
            )
        
        elif text == "🔗 Привязать ключ к юзеру":
            temp_auth[user_id] = {"step": "bind_key", "user_id": user_id}
            await m.reply(
                "🔗 Введите ключ и username в формате:\n"
                "`ключ-@username`\n\n"
                "Например: `KEY123-@durov`\n\n"
                "Этот ключ сможет использовать только пользователь с username @durov",
                reply_markup=ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)
            )
        
        elif text == "👥 Все пользователи":
            if not users_data:
                await m.reply("📭 Нет активных пользователей")
            else:
                users_list = "👥 Все пользователи:\n\n"
                for uid, data in users_data.items():
                    expires = data["expires"]
                    if isinstance(expires, str):
                        expires = datetime.fromisoformat(expires)
                    
                    accounts_count = len(data["accounts"])
                    bound_info = f" (привязан к @{data['bound_username']})" if data.get('bound_username') else ""
                    users_list += f"🆔 {uid}\n"
                    users_list += f"👤 {data.get('username', 'Нет username')}{bound_info}\n"
                    users_list += f"📱 Аккаунтов: {accounts_count}\n"
                    users_list += f"📅 Доступ до: {expires.strftime('%d.%m.%Y')}\n"
                    users_list += f"🔑 Ключ: {data['key_used']}\n"
                    users_list += f"👑 Админ: {'Да' if data['is_admin'] else 'Нет'}\n\n"
                
                if len(users_list) > 4000:
                    for i in range(0, len(users_list), 4000):
                        await m.reply(users_list[i:i+4000])
                else:
                    await m.reply(users_list)
        
        elif text == "📊 Статистика":
            total_users = len(users_data)
            total_accounts = sum(len(data["accounts"]) for data in users_data.values())
            total_running = sum(
                sum(1 for acc in data["accounts"].values() if acc.get("running", False)) 
                for data in users_data.values()
            )
            
            current_keys = load_keys()
            total_keys = len(current_keys)
            used_keys = sum(1 for user_data in users_data.values() if user_data["key_used"] in current_keys)
            
            bound_keys = sum(1 for v in current_keys.values() if isinstance(v, str) and v.startswith('@'))
            
            stats_text = (
                f"📊 Общая статистика бота:\n\n"
                f"👥 Пользователей: {total_users}\n"
                f"📱 Всего аккаунтов: {total_accounts}\n"
                f"🟢 Активных рассылок: {total_running}\n"
                f"🔑 Всего ключей: {total_keys}\n"
                f"🔗 Привязанных ключей: {bound_keys}\n"
                f"✅ Использовано ключей: {used_keys}\n"
                f"📦 Осталось ключей: {total_keys - used_keys}\n"
            )
            
            await m.reply(stats_text)

async def finalize_user_account(uid, data, m):
    """Завершает добавление аккаунта"""
    user_id = data["user_id"]
    phone = data["phone"]
    session_name = os.path.join(WORK_DIR, 'sessions', f"{phone.replace('+', '').replace(' ', '')}_{user_id}")
    
    client = data["client"]
    
    # Добавляем обработчик отключения
    async def on_disconnect(client, user_id=user_id, phone=phone):
        logger.warning(f"⚠️ Аккаунт {phone} отключился")
        await schedule_reconnect(user_id, phone)
    
    client.add_handler(DisconnectHandler(on_disconnect))
    
    # Запускаем keep-alive
    task_key = f"{user_id}_{phone}"
    if task_key in keep_alive_tasks:
        keep_alive_tasks[task_key].cancel()
    keep_alive_tasks[task_key] = asyncio.create_task(
        keep_alive(user_id, phone, client)
    )
    
    # Сохраняем в данные пользователя
    users_data[user_id]["accounts"][phone] = {
        "client": client,
        "text": "Привет! Это рассылка.",
        "interval": 3600,
        "running": False,
        "added_date": datetime.now().isoformat(),
        "session_name": session_name
    }
    
    await m.reply(f"✅ Аккаунт {phone} успешно добавлен!")
    
    # Очищаем временные данные
    if uid in temp_auth:
        temp_auth.pop(uid)
    
    save_users()
    
    logger.info(f"✅ Аккаунт {phone} добавлен для пользователя {user_id}")

# === ФУНКЦИЯ ГРАЦИОЗНОГО ЗАВЕРШЕНИЯ ===
async def shutdown(sig=None):
    """Грациозно завершает работу бота"""
    logger.info("🛑 Получен сигнал завершения, останавливаю бота...")
    
    # Останавливаем все keep-alive задачи
    for task in keep_alive_tasks.values():
        task.cancel()
    
    # Останавливаем все задачи переподключения
    for task in reconnect_tasks.values():
        task.cancel()
    
    # Сохраняем данные перед выходом
    save_users()
    
    # Останавливаем всех клиентов
    for user_id, user_data in users_data.items():
        for phone, acc in user_data["accounts"].items():
            if "client" in acc:
                try:
                    await acc["client"].stop()
                    logger.info(f"✅ Клиент {phone} остановлен")
                except:
                    pass
    
    # Останавливаем бота
    await bot.stop()
    
    logger.info("👋 Бот завершил работу")
    sys.exit(0)

if __name__ == "__main__":
    # Регистрируем обработчики сигналов для грациозного завершения
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig)))
    
    # Создаем необходимые папки
    os.makedirs(os.path.join(WORK_DIR, "sessions"), exist_ok=True)
    os.makedirs(os.path.join(WORK_DIR, "user_settings"), exist_ok=True)
    os.makedirs(os.path.join(WORK_DIR, "bot_session"), exist_ok=True)
    
    # Загружаем данные
    load_users()
    
    # Запускаем загрузку сессий
    loop = asyncio.get_event_loop()
    
    async def startup():
        logger.info("🚀 Запуск бота...")
        
        # Проверяем доступность Volume на Railway
        if IS_RAILWAY:
            test_file = os.path.join(WORK_DIR, 'test_write.txt')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                logger.info("✅ Volume доступен для записи")
            except Exception as e:
                logger.error(f"❌ Volume НЕ доступен для записи: {e}")
                logger.error("Проверьте настройки Volume в Railway!")
        
        # Загружаем сессии
        loaded = await load_user_sessions()
        current_keys = load_keys()
        logger.info(f"🔑 Доступные ключи: {list(current_keys.keys())}")
        logger.info(f"👥 Пользователей: {len(users_data)}")
        logger.info(f"📱 Активных сессий: {loaded}")
    
    loop.run_until_complete(startup())
    
    # Запускаем бота
    logger.info("🤖 Бот запущен и готов к работе")
    
    try:
        bot.run()
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(shutdown())
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        loop.run_until_complete(shutdown())
