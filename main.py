import aiogram
from aiogram import Bot, types, Dispatcher
import asyncpg
import asyncio
from datetime import datetime, timedelta
import pytz
from aiogram.types import ChatMemberUpdated
from middleware import SimpleLoggerMiddleware
from aiogram import Router
from aiogram.filters import IS_MEMBER, IS_NOT_MEMBER, Command, ChatMemberUpdatedFilter
from aiogram.handlers import MessageHandler

API_TOKEN = '6630774756:AAFTsxAbj7uM6Woz5a7BtWkMXcrEVd6Gdt8'
DB_CONFIG = {
            'host': 'ec2-54-73-22-169.eu-west-1.compute.amazonaws.com',    
            'port': 5432,    
            'user': 'ydcclkyluxxblw',
            'password': '10984c6b9c775b1020d496f434fb20b3f2d7bb7dd590774c4cde22c513fcdb24',    
            'db': 'd7kb3aae5a9311',}

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()
router.message.middleware(SimpleLoggerMiddleware())
dp.include_router(router)


async def get_notifications():
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        notifications = await conn.fetch("""
            SELECT m.id, m.deadline, m.message_text, g.chat_id, m.num_of_notif 
            FROM notificator.messages m
            JOIN notificator.notifications_groups ng ON ng.notif_id = m.id
            RIGHT JOIN notificator.student_groups g ON g.id = ng.group_id
            WHERE m.is_relevant = 1
            ORDER BY m.id DESC;
        """)
    finally:
        await conn.close()
    return notifications


async def send_notifications():
    notifications = await get_notifications()
    tz_utc_plus_6 = pytz.timezone('Asia/Almaty')
    now = datetime.now(tz=tz_utc_plus_6)
    for notification in notifications:
        deadline = notification['deadline']
        message_text = "Еске саламын!\nНапоминаю!\nReminder!\n" + notification['message_text'] + f"Дедлайн/Deadline: {deadline}"
        chat_id = notification['chat_id']
        num_of_notif = notification['num_of_notif']
        if (now >= deadline - timedelta(days=2) and num_of_notif < 2) or \
                (now >= deadline - timedelta(days=1) and num_of_notif < 3) or \
                now >= deadline - timedelta(hours=6):
            if now >= deadline - timedelta(hours=6):
                await mark_notification(notification, False)
            else:
                await mark_notification(notification)
            try:
                await bot.send_message(chat_id, message_text)
            except Exception as e:
                print(f"Error sending message: {e}")


async def mark_notification(notification, is_relevant = True):
    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                UPDATE notificator.messages 
                SET num_of_notif = $1, is_relevant = $2 WHERE id = $3;
                """,
                notification['num_of_notif'] + 1, is_relevant, notification['id']
            )
        except Exception as e:
            print(f"Error updating data: {e}")
        finally:
            await pool.close()
    

@router.my_chat_member(ChatMemberUpdatedFilter(IS_NOT_MEMBER >> IS_MEMBER))
async def on_user_join(event: ChatMemberUpdated):
    bot_user_id = bot.id 
    if event.new_chat_member.user.id == bot_user_id:
        chat_id = event.chat.id
        if event.chat.type not in ['group', 'supergroup']:
            await bot.send_message(event.chat.id,

"""Өкінішке орай, мен тек топтық чаттарда жұмыс істеймін.

К сожалению, я работаю только в групповых чатах.

Unfortunately, I only work in group chats.
            """)
        else:
            group_name = event.chat.title
            await set_group_name(group_name, event.chat.id)
            response_message = f"""Сәлеметсіздер! 
Мен Исаханова Асель Алимахановнадан сіздің тобыңызға хабарламалар жіберетін боламын. 
Сіздің тобыңыз "{group_name}" деген атпен қосылды. 
Егер сіз оның атауын өзгерткіңіз келсе, топтың атауын өзгертіңіз, содан кейін /rename командасын жіберіңіз.

Рад вас приветствовать! 
Я буду отправлять уведомления в вашу группу от Исахановой Асель Алимахановны.
Ваша группа была добавлена под названием "{group_name}".
Если вы хотите изменить ее название, то измените название группы , затем отправьте команду /rename.

Welcome! I will be sending notifications to your group from Asel Alimakhanova Isakhanova. 
Your group has been added under the name "{group_name}". 
If you want to change its name, modify the group name, then send the /rename command."""

            await bot.send_message(event.chat.id, response_message)


@router.my_chat_member(ChatMemberUpdatedFilter(IS_MEMBER >> IS_NOT_MEMBER))
async def on_user_leave(event: ChatMemberUpdated):
    bot_user_id = bot.id 
    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with pool.acquire() as conn:
        if event.old_chat_member.user.id == bot_user_id:
            try:
                await conn.execute(
                    """
                    UPDATE student_groups 
                    SET is_available = 0 
                    WHERE chat_id = $1;
                    """,
                    event.chat.id
                )
            except Exception as e:
                print(f"Error updating data: {e}")
            finally:
                await pool.close()
        



async def set_group_name(group_name, chat_id):
    pool = await asyncpg.create_pool(**DB_CONFIG)
    async with pool.acquire() as conn:
        existing_group = await conn.fetchrow(
            "SELECT * FROM student_groups WHERE chat_id = $1", chat_id
        )
        if existing_group:
            await conn.execute(
                """
                UPDATE student_groups 
                SET group_name = $1, is_available = 1 
                WHERE chat_id = $2
                """,
                group_name, chat_id
            )
        else:
            await conn.execute(
                """
                INSERT INTO student_groups (chat_id, group_name, is_banned, is_available) 
                VALUES ($1, $2, FALSE, TRUE)
                """,
                chat_id, group_name
            )
        await pool.close()



# @router.message(Command('start'))
# async def process_start_command(message: types.Message):
#     print("Start { " + message + "\n}")
    

@router.message(Command('rename'))
async def process_start_command(message: types.Message):
    group_name = message.chat.title
    await set_group_name(group_name, message.chat.id)
    response_message = f"""Сіздің тобыңыз "{group_name}" деген атпен жаңартылды. 
Егер сіз оның атауын өзгерткіңіз келсе, топтың атауын өзгертіңіз, содан кейін /rename командасын жіберіңіз.

Ваша группа была обновлена под названием "{group_name}".
Если вы хотите изменить её название, то измените название группы , затем отправьте команду /rename.

Your group has been updated with the name "{group_name}". 
If you want to change its name, modify the group name, then send the /rename command."""
    await bot.send_message(message.chat.id, response_message)


async def scheduler():
    while True:
        await send_notifications()
        await asyncio.sleep(10)


async def on_startup(_):
    asyncio.create_task(scheduler())


if __name__ == '__main__':
    asyncio.run(dp.start_polling(bot, on_startup=on_startup))
