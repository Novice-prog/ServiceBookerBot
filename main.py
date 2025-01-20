import os
import asyncio
import logging
import re
import aiosqlite
import pytz
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, BaseMiddleware, Router
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, CallbackQuery, TelegramObject
)
from aiogram.client.bot import DefaultBotProperties
from dotenv import load_dotenv

# Для работы с Google Calendar
from google.oauth2 import service_account
from googleapiclient.discovery import build

# LangChain/GigaChat
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_gigachat.chat_models import GigaChat

load_dotenv()

logging.basicConfig(force=True, level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================================================
# Конфигурация
# ==================================================
DATABASE = 'users.db'

TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
GIGACHAT_API_KEY = os.getenv('GIGACHAT_API_KEY')

# Параметры для Google Calendar
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE', 'test-fortgbot-2312e65c9aec.json')
CALENDAR_ID = os.getenv('CALENDAR_ID')  # ID календаря
TIMEZONE = 'Europe/Moscow'  # Часовой пояс

# Проверка необходимых ключей
if not TELEGRAM_BOT_TOKEN or not GIGACHAT_API_KEY:
    logger.error("Отсутствуют необходимые ключи для Telegram Bot или GigaChat.")
    exit(1)


# ==================================================
# Класс для работы с Google Calendar
# ==================================================
class GC:
    SCOPES = ['https://www.googleapis.com/auth/calendar']

    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            filename=SERVICE_ACCOUNT_FILE, scopes=self.SCOPES
        )
        self.service = build('calendar', 'v3', credentials=credentials)

    def add_event(self, calendar_id, body):
        return self.service.events().insert(
            calendarId=calendar_id,
            body=body
        ).execute()

    def delete_event(self, calendar_id, event_id):
        """
        Удаляет событие из календаря по его event_id.
        """
        return self.service.events().delete(
            calendarId=calendar_id,
            eventId=event_id
        ).execute()

    def check_availability(self, calendar_id, start_time, end_time):
        events_result = self.service.events().list(
            calendarId=calendar_id,
            timeMin=start_time.isoformat(),
            timeMax=end_time.isoformat(),
            singleEvents=True,
            orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        return len(events) == 0


# Создаём глобальный объект для работы с календарём
calendar_service = GC()


# ==================================================
# Функции для работы с Google Calendar (асинхронные)
# ==================================================
async def save_to_calendar(appointment_id: int, user_id: int, service_name: str, date_str: str, time_str: str,
                           user_phone: str):
    """
    Сохраняет запись в Google Calendar.
    date_str + time_str у нас в формате: "дд.мм.гг чч:мм:сс".
    Для Calendar нужно преобразовать в datetime и затем в ISO.

    После успешного создания события, его id (google_event_id) сохраняется в БД,
    чтобы при отмене записи удалить и из календаря.
    """
    try:
        tz = pytz.timezone(TIMEZONE)
        # Если у вас 4-значный год, поменяйте на '%d.%m.%Y %H:%M:%S'
        dt = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%y %H:%M:%S")
        start_time = tz.localize(dt)
        end_time = start_time + timedelta(hours=1)

        # Получаем имя/фамилию из БД
        async with aiosqlite.connect(DATABASE) as db:
            cursor = await db.execute("SELECT first_name, last_name FROM users WHERE id = ?", (user_id,))
            row = await cursor.fetchone()
            if row:
                first_name, last_name = row
            else:
                first_name, last_name = 'Неизвестно', ''

        # Получаем username
        user = await bot.get_chat(user_id)
        username = user.username if user.username else f"tg://user?id={user_id}"
        telegram_link = f"https://t.me/{username}"

        event = {
            'summary': service_name,
            'description': (
                f"Имя: {first_name} {last_name}\n"
                f"Телефон: {user_phone}\n"
                f"Telegram: {telegram_link}"
            ),
            'start': {
                'dateTime': start_time.isoformat(),
                'timeZone': TIMEZONE,
            },
            'end': {
                'dateTime': end_time.isoformat(),
                'timeZone': TIMEZONE,
            },
        }

        loop = asyncio.get_running_loop()
        event_result = await loop.run_in_executor(
            None, lambda: calendar_service.add_event(CALENDAR_ID, event)
        )
        logger.info(f"Event created: {event_result.get('htmlLink')}")

        # Сохраняем google_event_id в БД
        google_event_id = event_result.get('id')
        async with aiosqlite.connect(DATABASE) as db:
            await db.execute('''
                UPDATE appointments
                SET google_event_id = ?
                WHERE id = ?
            ''', (google_event_id, appointment_id))
            await db.commit()

        return start_time, end_time
    except Exception as e:
        logger.error(f"Error creating calendar event for user {user_id}: {e}")
        await bot.send_message(
            chat_id=user_id,
            text="Произошла ошибка при добавлении записи в календарь. Пожалуйста, попробуйте позже."
        )
        return None, None


async def check_availability(date: str, time_: str) -> bool:
    """
    Проверяем, доступен ли слот (дд.мм.гг чч:мм:сс) в Google Calendar.
    """
    try:
        tz = pytz.timezone(TIMEZONE)
        dt = datetime.strptime(f"{date} {time_}", "%d.%m.%y %H:%M:%S")
        start_time = tz.localize(dt)
        end_time = start_time + timedelta(hours=1)

        logger.info(f"Checking availability: start={start_time.isoformat()}, end={end_time.isoformat()}")

        loop = asyncio.get_running_loop()
        is_available = await loop.run_in_executor(
            None, lambda: calendar_service.check_availability(CALENDAR_ID, start_time, end_time)
        )
        return is_available
    except Exception as e:
        logger.error(f"Error in check_availability: {e}")
        return False


# ==================================================
# Инициализируем бота
# ==================================================
bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)  # Установка режима парсинга по умолчанию
)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# ==================================================
# GigaChat
# ==================================================
llm = GigaChat(
    credentials=GIGACHAT_API_KEY,
    scope="GIGACHAT_API_PERS",
    model="GigaChat",
    verify_ssl_certs=False,
    streaming=False,
)


# ==================================================
# Состояния для регистрации
# ==================================================
class Registration(StatesGroup):
    first_name = State()
    last_name = State()
    phone = State()


# ==================================================
# Middleware для проверки регистрации
# ==================================================
class RegistrationMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        if isinstance(event, Message) and event.text != '/start':
            user_id = event.from_user.id  # <-- Всегда from_user.id, а не chat.id
            state = data.get('state')
            if state:
                current_state = await state.get_state()
                if current_state is None:
                    # Проверяем, есть ли user_id в БД
                    async with aiosqlite.connect(DATABASE) as db:
                        async with db.execute(
                                "SELECT id FROM users WHERE id = ?",
                                (user_id,)
                        ) as cursor:
                            if await cursor.fetchone() is None:
                                await bot.send_message(
                                    user_id,
                                    "Вы не зарегистрированы! Введите /start для начала регистрации."
                                )
                                return
        return await handler(event, data)


# ==================================================
# Инициализация БД
# ==================================================
async def start_db():
    async with aiosqlite.connect(DATABASE) as db:
        # Таблица пользователей
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                phone TEXT,
                statusrem BOOLEAN,
                pending_appointment_id INTEGER
            )
        ''')
        # Таблица записей
        await db.execute('''
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                service TEXT,
                date_time TEXT,   -- дд.мм.гг чч:мм:сс
                status TEXT DEFAULT 'pending',
                created_at TEXT,
                google_event_id TEXT,
                reminded BOOLEAN DEFAULT 0
            )
        ''')

        # Если таблица уже создана, возможно столбцов нет – добавим через ALTER
        # Это защитит от ошибок при обновлении кода.
        try:
            await db.execute("ALTER TABLE appointments ADD COLUMN google_event_id TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE appointments ADD COLUMN reminded BOOLEAN DEFAULT 0")
        except:
            pass

        await db.commit()


# ==================================================
# Команда /start (регистрация или меню)
# ==================================================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    async with aiosqlite.connect(DATABASE) as db:
        async with db.execute("SELECT id FROM users WHERE id = ?", (user_id,)) as cursor:
            if await cursor.fetchone() is None:
                # Начинаем регистрацию
                await state.set_state(Registration.first_name)
                await message.answer("Привет! Начнём регистрацию. Как вас зовут (имя)?")
            else:
                # Уже зарегистрирован
                await message.answer(
                    "Добро пожаловать! Выберите действие:",
                    reply_markup=main_menu_keyboard()
                )


@dp.message(Registration.first_name)
async def reg_first_name(message: Message, state: FSMContext):
    first_name = message.text.strip()
    if not first_name:
        await message.answer("Пожалуйста, введите имя.")
        return
    await state.update_data(first_name=first_name)
    await state.set_state(Registration.last_name)
    await message.answer("Введите фамилию.")


@dp.message(Registration.last_name)
async def reg_last_name(message: Message, state: FSMContext):
    last_name = message.text.strip()
    if not last_name:
        await message.answer("Пожалуйста, введите фамилию.")
        return
    await state.update_data(last_name=last_name)
    await state.set_state(Registration.phone)
    await message.answer("Введите номер телефона в формате +123456789.")


@dp.message(Registration.phone)
async def reg_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if not re.match(r'^\+\d{9,15}$', phone):
        await message.answer("Неверный формат номера. Укажите в формате +123456789.")
        return

    data = await state.get_data()
    first_name = data.get('first_name')
    last_name = data.get('last_name')

    async with aiosqlite.connect(DATABASE) as db:
        await db.execute('''
            INSERT INTO users (id, first_name, last_name, phone, statusrem, pending_appointment_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (message.from_user.id, first_name, last_name, phone, False, None))
        await db.commit()

    await state.clear()
    await message.answer("Регистрация успешно завершена!", reply_markup=ReplyKeyboardRemove())
    await message.answer("Выберите действие:", reply_markup=main_menu_keyboard())


# ==================================================
# Главное меню
# ==================================================
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    btn1 = KeyboardButton(text="Записаться")
    btn2 = KeyboardButton(text="Мои Записи")
    btn3 = KeyboardButton(text="Помощь")
    return ReplyKeyboardMarkup(
        keyboard=[[btn1, btn2], [btn3]],
        resize_keyboard=True
    )


@router.message(lambda msg: msg.text in ["Записаться", "Мои Записи", "Помощь"])
async def handle_main_menu(message: Message):
    if message.text == "Записаться":
        # Русские названия услуг
        b1 = InlineKeyboardButton(text="Маникюр", callback_data="service_manicure")
        b2 = InlineKeyboardButton(text="Педикюр", callback_data="service_pedicure")
        b3 = InlineKeyboardButton(text="Брови", callback_data="service_eyebrows")
        b4 = InlineKeyboardButton(text="Ресницы", callback_data="service_eyelashes")

        markup = InlineKeyboardMarkup(inline_keyboard=[[b1, b2], [b3, b4]])
        await message.answer("Выберите услугу:", reply_markup=markup)

    elif message.text == "Мои Записи":
        user_id = message.from_user.id
        async with aiosqlite.connect(DATABASE) as db:
            cursor = await db.execute('''
                SELECT id, service, date_time, status, created_at
                FROM appointments
                WHERE user_id = ?
                ORDER BY id DESC
            ''', (user_id,))
            rows = await cursor.fetchall()

        if rows:
            messages = []
            for row in rows:
                app_id, service, date_time, status, created_at = row

                text_block = (
                    f"<b>Услуга:</b> {service}\n"
                    f"<b>Дата/Время:</b> {date_time if date_time else '—'}\n"
                    f"<b>Статус:</b> {status}\n"
                    f"<b>Создано:</b> {created_at}"
                )

                # Если статус не canceled, добавим кнопку "Отменить"
                if status != "canceled":
                    cancel_btn = InlineKeyboardButton(
                        text="Отменить",
                        callback_data=f"cancel_app_{app_id}"
                    )
                    kb = InlineKeyboardMarkup(inline_keyboard=[[cancel_btn]])
                    messages.append((text_block, kb))
                else:
                    messages.append((text_block, None))

            for block, keyboard in messages:
                await message.answer(block, reply_markup=keyboard)
        else:
            await message.answer("У вас пока нет записей.")

    elif message.text == "Помощь":
        await message.answer("Добро пожаловать в службу поддержки! Мы понимаем, что не работает — мы тоже в шоке, но не переживайте!")


# ==================================================
# Создание черновой записи (pending)
# ==================================================
async def create_pending_appointment(user_id: int, service: str) -> int:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    async with aiosqlite.connect(DATABASE) as db:
        c = await db.execute("SELECT pending_appointment_id FROM users WHERE id = ?", (user_id,))
        row = await c.fetchone()
        old_appointment_id = row[0] if row else None

        # Удаляем старую, если есть (и она в статусе 'pending')
        if old_appointment_id:
            await db.execute(
                "DELETE FROM appointments WHERE id = ? AND status='pending'",
                (old_appointment_id,)
            )

        # Определяем русское название услуги
        if service == "manicure":
            service_name = "Маникюр"
        elif service == "pedicure":
            service_name = "Педикюр"
        elif service == "eyebrows":
            service_name = "Брови"
        elif service == "eyelashes":
            service_name = "Ресницы"
        else:
            service_name = service  # fallback

        cursor = await db.execute('''
            INSERT INTO appointments (user_id, service, date_time, status, created_at)
            VALUES (?, ?, ?, 'pending', ?)
        ''', (user_id, service_name, None, now_str))
        new_id = cursor.lastrowid

        await db.execute(
            "UPDATE users SET pending_appointment_id = ? WHERE id = ?",
            (new_id, user_id)
        )
        await db.commit()
    return new_id


# ==================================================
# Хендлер inline-кнопок выбора услуги
# ==================================================
@router.callback_query(lambda c: c.data and c.data.startswith("service_"))
async def handle_service_choice(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    service_code = callback_query.data.split("_", maxsplit=1)[1]

    appointment_id = await create_pending_appointment(user_id, service_code)


    await callback_query.answer()
    rus_name = {
        "manicure": "Маникюр",
        "pedicure": "Педикюр",
        "eyebrows": "Брови",
        "eyelashes": "Ресницы"
    }.get(service_code, service_code)

    await callback_query.message.answer(
        f"Вы выбрали услугу: <b>{rus_name}</b>.\n\n"
        "Введите удобные дату и время (например: «25.01.2025 в 14:30»)."
    )


# ==================================================
# Парсинг даты/времени -> дд.мм.гг чч:мм:сс
# ==================================================
def parse_and_format_datetime(date_str: str, time_str: str) -> str:
    """
    Ожидаем, что GigaChat вернёт дату типа '25.01.2025' и время '14:30'.
    Приводим к формату 'дд.мм.гг чч:мм:сс'.
    Если используем 4-значный год, меняем '%d.%m.%y' на '%d.%m.%Y'.
    """
    # Попробуем несколько форматов:
    for date_fmt in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M"):
        try:
            dt = datetime.strptime(f"{date_str} {time_str}", date_fmt)
            return dt.strftime("%d.%m.%y %H:%M:%S")
        except ValueError:
            pass

    return ""


# ==================================================
# Хендлер произвольного текста (ожидаем дату/время)
# ==================================================
@router.message()
async def handle_free_text(message: Message):
    user_id = message.from_user.id
    user_input = message.text.strip()

    # Смотрим, есть ли pending-запись
    async with aiosqlite.connect(DATABASE) as db:
        c = await db.execute("SELECT pending_appointment_id FROM users WHERE id=?", (user_id,))
        row = await c.fetchone()
        pending_appointment_id = row[0] if row else None

    if pending_appointment_id:
        # Отправляем запрос GigaChat
        system_prompt = SystemMessage(
            content="Ты – ассистент, извлекающий дату и время из текста."
        )
        user_prompt = HumanMessage(
            content=(
                    "Текст пользователя: «" + user_input + "»\n\n"
                                                           "Требуется выделить предполагаемую дату и время. "
                                                           "Если в дате не указан год, то ставь по умолчанию 2025."
                                                            "Формат ответа: DATE: <дд.чч.гггг> TIME: <время>, "
                                                           "или NOT_FOUND, если не удалось определить."
            )
        )
        response = llm([system_prompt, user_prompt])
        content = response.content.strip()
        logger.info(f"GigaChat response: {content}")

        if "NOT_FOUND" in content.upper():
            await message.answer("Не удалось понять дату/время. Попробуйте снова.")
            return

        date_match = re.search(r"DATE:\s*(.+?)(?:\s|$)", content, re.IGNORECASE)
        time_match = re.search(r"TIME:\s*(.+?)(?:\s|$)", content, re.IGNORECASE)

        extracted_date = date_match.group(1).strip() if date_match else None
        extracted_time = time_match.group(1).strip() if time_match else None

        if not (extracted_date and extracted_time):
            await message.answer("Не удалось извлечь дату/время. Попробуйте снова.")
            return

        final_dt_str = parse_and_format_datetime(extracted_date, extracted_time)
        if not final_dt_str:
            await message.answer("Не получилось привести дату/время к нужному формату. Попробуйте ещё раз.")
            return

        # Проверяем доступность в календаре
        is_free = await check_availability(*final_dt_str.split(" ", 1))
        if not is_free:
            await message.answer(
                "К сожалению, выбранное время уже занято. Попробуйте указать другое время."
            )
            return

        # Записываем дату/время в БД
        async with aiosqlite.connect(DATABASE) as db:
            await db.execute('''
                UPDATE appointments
                SET date_time = ?
                WHERE id = ? AND user_id = ?
            ''', (final_dt_str, pending_appointment_id, user_id))
            await db.commit()

        # Предлагаем подтвердить
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Да", callback_data=f"confirm_{pending_appointment_id}"),
            InlineKeyboardButton(text="Нет", callback_data=f"cancel_{pending_appointment_id}")
        ]])
        await message.answer(
            f"Вы выбрали дату и время: <b>{final_dt_str}</b>\nПодтверждаете запись?",
            reply_markup=kb
        )
    else:
        await message.answer("Выберите действие в меню или введите /start для начала работы.")


# ==================================================
# Обработчики подтверждения/отмены (confirm / cancel)
# ==================================================
@router.callback_query(lambda c: c.data and (c.data.startswith("confirm_")))
async def handle_confirmation(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    action, appointment_id_str = data.split("_", maxsplit=1)
    appointment_id = int(appointment_id_str.split('_')[-1])

    async with aiosqlite.connect(DATABASE) as db:
        cursor = await db.execute(
            "SELECT status, date_time, service FROM appointments WHERE id = ? AND user_id = ?",
            (appointment_id, user_id)
        )
        row = await cursor.fetchone()
        if not row:
            await callback_query.answer("Запись не найдена.", show_alert=True)
            return

        status, date_time_str, service_name = row
        if status != "pending":
            await callback_query.answer("Эта запись уже подтверждена/отменена или недоступна.", show_alert=True)
            return

        if action == "confirm":
            # Подтверждаем -> status='confirmed'
            await db.execute('''
                UPDATE appointments
                SET status='confirmed'
                WHERE id = ? AND user_id = ?
            ''', (appointment_id, user_id))

            # Сбрасываем pending_appointment_id
            await db.execute('''
                UPDATE users
                SET pending_appointment_id = NULL
                WHERE id = ?
            ''', (user_id,))

            # Получаем телефон
            c2 = await db.execute("SELECT phone FROM users WHERE id = ?", (user_id,))
            row2 = await c2.fetchone()
            user_phone = row2[0] if row2 else 'Неизвестно'
            await db.commit()

            await callback_query.answer("Запись подтверждена!", show_alert=False)
            await callback_query.message.answer(
                f"Отлично! Ваша запись на <b>{service_name}</b> "
                f"в {date_time_str} подтверждена."
            )

            # Сохраняем в календарь (передаём appointment_id, чтобы потом при отмене удалить и из календаря)
            date_part, time_part = date_time_str.split(" ", 1)  # 'дд.мм.гг', 'чч:мм:сс'
            await save_to_calendar(
                appointment_id=appointment_id,
                user_id=user_id,
                service_name=service_name,
                date_str=date_part,
                time_str=time_part,
                user_phone=user_phone
            )

        elif action == "cancel":
            # Удаляем запись со статус 'pending'
            await db.execute('DELETE FROM appointments WHERE id = ? AND user_id = ?', (appointment_id, user_id))
            await db.execute('''
                UPDATE users
                SET pending_appointment_id = NULL
                WHERE id = ?
            ''', (user_id,))
            await db.commit()

            await callback_query.answer("Запись отменена.", show_alert=False)
            await callback_query.message.answer("Вы отменили текущую запись. Можете заново выбрать услугу.")


@router.callback_query(lambda c: c.data and c.data.startswith("cancel_"))
async def handle_confirmation(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    action, appointment_id_str = data.split("_", maxsplit=1)
    appointment_id = int(appointment_id_str.split('_')[-1])

    async with aiosqlite.connect(DATABASE) as db:
        cursor = await db.execute(
            "SELECT status, date_time, service, google_event_id FROM appointments WHERE id = ? AND user_id = ?",
            (appointment_id, user_id)
        )
        row = await cursor.fetchone()
        if not row:
            await callback_query.answer("Запись не найдена.", show_alert=True)
            return

        status, date_time_str, service_name, google_event_id = row
        if status == "pending":
            await callback_query.answer("Эта запись уже подтверждена/отменена или недоступна.", show_alert=True)
            return

        if action == "confirm":
            # Подтверждаем запись, изменяя статус на 'confirmed'
            await db.execute('''UPDATE appointments SET status='confirmed' WHERE id = ? AND user_id = ?''',
                             (appointment_id, user_id))

            # Сбрасываем pending_appointment_id у пользователя
            await db.execute('''UPDATE users SET pending_appointment_id = NULL WHERE id = ?''', (user_id,))

            # Получаем номер телефона пользователя
            c2 = await db.execute("SELECT phone FROM users WHERE id = ?", (user_id,))
            row2 = await c2.fetchone()
            user_phone = row2[0] if row2 else 'Неизвестно'
            await db.commit()

            await callback_query.answer("Запись подтверждена!", show_alert=False)
            await callback_query.message.answer(
                f"Отлично! Ваша запись на <b>{service_name}</b> в {date_time_str} подтверждена."
            )

            # Сохраняем в Google Calendar
            date_part, time_part = date_time_str.split(" ", 1)  # 'дд.мм.гг', 'чч:мм:сс'
            await save_to_calendar(
                appointment_id=appointment_id,
                user_id=user_id,
                service_name=service_name,
                date_str=date_part,
                time_str=time_part,
                user_phone=user_phone
            )

        elif action == "cancel":
            # Удаляем запись со статусом 'pending'
            await db.execute('DELETE FROM appointments WHERE id = ? AND user_id = ?', (appointment_id, user_id))
            await db.execute('''UPDATE users SET pending_appointment_id = NULL WHERE id = ?''', (user_id,))
            await db.commit()

            # Удаляем событие из Google Calendar, если оно существует
            if google_event_id:
                loop = asyncio.get_running_loop()
                try:
                    # Вызываем функцию для удаления события из Google Calendar
                    await loop.run_in_executor(
                        None,
                        lambda: calendar_service.delete_event(CALENDAR_ID, google_event_id)
                    )
                    logger.info(f"Событие с ID {google_event_id} успешно удалено из Google Календаря.")
                except Exception as e:
                    logger.error(f"Не удалось удалить событие из Google Календаря: {e}")
                    await callback_query.answer("Не удалось удалить событие из Google Календаря. Попробуйте позже.",
                                                show_alert=True)
                    return

            await callback_query.answer("Запись отменена.", show_alert=False)
            await callback_query.message.answer("Вы отменили текущую запись. Можете заново выбрать услугу.")



# ==================================================
# Напоминание пользователям за 2 часа до услуги
# ==================================================
async def reminder_scheduler():
    """
    Каждую минуту проверяем записи, у которых статус = 'confirmed',
    до начала которых осталось <= 2 часа, и отправляем напоминание, если ещё не отправляли.
    """
    while True:
        try:
            now = datetime.now(pytz.timezone(TIMEZONE))
            async with aiosqlite.connect(DATABASE) as db:
                # Ищем неподтверждённые напоминания для confirmed-записей
                # date_time хранится в формате 'дд.мм.гг чч:мм:сс'
                cursor = await db.execute('''
                    SELECT id, user_id, service, date_time, reminded
                    FROM appointments
                    WHERE status = 'confirmed' AND reminded = 0
                ''')
                rows = await cursor.fetchall()

            for row in rows:
                app_id, user_id, service_name, dt_str, reminded = row

                # Преобразуем дату/время
                try:
                    dt = datetime.strptime(dt_str, "%d.%m.%y %H:%M:%S")
                    dt = pytz.timezone(TIMEZONE).localize(dt)
                except ValueError:
                    continue  # некорректный формат, пропустим

                if dt <= now:
                    # Если запись уже в прошлом, смысла напоминать нет
                    continue

                diff = dt - now
                # Если до начала <= 2 часов
                if 0 < diff.total_seconds() <= 7200:
                    # Отправляем напоминание
                    try:
                        await bot.send_message(
                            user_id,
                            f"Напоминание! Ваша запись на <b>{service_name}</b> начинается через ~2 часа.\n"
                            f"Дата и время: {dt_str}"
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить напоминание пользователю {user_id}: {e}")
                        continue

                    # Обновляем флаг reminded
                    async with aiosqlite.connect(DATABASE) as db:
                        await db.execute(
                            "UPDATE appointments SET reminded=1 WHERE id=?",
                            (app_id,)
                        )
                        await db.commit()

        except Exception as e:
            logger.error(f"Ошибка в планировщике напоминаний: {e}")

        # Ждём 60 секунд до следующего цикла
        await asyncio.sleep(60)


# ==================================================
# Запуск бота
# ==================================================
async def main():
    await start_db()

    # Запускаем планировщик напоминаний
    asyncio.create_task(reminder_scheduler())

    dp.message.outer_middleware(RegistrationMiddleware())
    dp.include_router(router)

    try:
        print("Бот запущен...")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        print("Бот остановлен")


if __name__ == "__main__":
    asyncio.run(main())