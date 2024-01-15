import aiogram
from aiogram import Bot, types, Dispatcher
import asyncpg
import asyncio
from datetime import datetime, timedelta, timezone
import pytz
from aiogram.types import ChatMemberUpdated
from middleware import SimpleLoggerMiddleware
from aiogram import Router
from aiogram.filters import IS_MEMBER, IS_NOT_MEMBER, Command, ChatMemberUpdatedFilter
from aiogram.handlers import MessageHandler
import logging
import pytz

logging.basicConfig(level=logging.DEBUG)


API_TOKEN = '6630774756:AAFTsxAbj7uM6Woz5a7BtWkMXcrEVd6Gdt8'
DB_CONFIG = {
            'host': 'ec2-54-73-22-169.eu-west-1.compute.amazonaws.com',    
            'port': 5432,    
            'user': 'ydcclkyluxxblw',
            'password': '10984c6b9c775b1020d496f434fb20b3f2d7bb7dd590774c4cde22c513fcdb24',    
            'database': 'd7kb3aae5a9311'
            }

pool = None
router = Router()
dp = Dispatcher()
bot = Bot(token=API_TOKEN)



async def get_notifications():
    async with pool.acquire() as conn:
        try:
            notifications = await conn.fetch("""
                SELECT m.id,  m.deadline, m.message_text, g.chat_id, m.num_of_notif 
                FROM messages m
                JOIN notifications_groups ng ON ng.notif_id = m.id
                RIGHT JOIN student_groups g ON g.id = ng.group_id
                WHERE m.is_relevant = true
                ORDER BY m.id DESC;
            """)
            return notifications
        except Exception as e:
            print(f"Error in set_group_name: {e}")


async def send_notifications():
    notifications = await get_notifications()
    if notifications is None:
        return

    tz_utc_plus_6 = pytz.timezone('Asia/Almaty')
    now = datetime.now(tz=tz_utc_plus_6)

    for notification in notifications:
        deadline = notification['deadline'].replace(tzinfo=timezone(timedelta(hours=6)))
        formatted_deadline = deadline.strftime('%d.%m.%Y %H:%M')
        
        six_hours_before = deadline - timedelta(hours=6)
        
        dif = deadline - now

        message_text = f"<b>Еске саламын!\nНапоминаю!\nReminder!\n\n</b><i>{notification['message_text']}</i>\n\n<u>Дедлайн/Deadline:</u> {formatted_deadline}"
        
        num_of_notif = notification['num_of_notif']
        
        should_mark = (num_of_notif < 2 and dif.days == 3) or \
                    (num_of_notif < 3 and dif.days == 1) or \
                    now >= six_hours_before
        

        if should_mark:
            if now >= six_hours_before:
                await mark_notification(notification, False)
            else:
                await mark_notification(notification)
            
            try:
                await bot.send_message(notification['chat_id'], message_text, parse_mode="HTML")
            except Exception as e:
                print(f"Error sending message: {e}")


async def mark_notification(notification, is_relevant = True):
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                UPDATE messages 
                SET num_of_notif = $1, is_relevant = $2 WHERE id = $3;
                """,
                notification['num_of_notif'] + 1, is_relevant, notification['id']
            )
        except Exception as e:
            print(f"Error updating data: {e}")
    

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
    async with pool.acquire() as conn:
        if event.old_chat_member.user.id == bot_user_id:
            try:
                await conn.execute(
                    """
                    UPDATE student_groups 
                    SET is_available = false 
                    WHERE chat_id = $1;
                    """,
                    event.chat.id
                )
            except Exception as e:
                print(f"Error updating data: {e}")
        



async def set_group_name(group_name, chat_id):
    async with pool.acquire() as conn:
        existing_group = await conn.fetchrow(
            "SELECT * FROM student_groups WHERE chat_id = $1", chat_id
        )
        if existing_group:
            await conn.execute(
                """
                UPDATE student_groups 
                SET group_name = $1, is_available = true 
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
        await asyncio.sleep(1200)


async def on_startup(dispatcher):
    try:
        logging.info("Starting application...")
        global pool
        pool = await asyncpg.create_pool(**DB_CONFIG)
        logging.info("Database pool initialized.")
        asyncio.create_task(scheduler())
        logging.info("Application successfully started.")
    except Exception as e:
        print(f"Error during startup: {e}")


async def main():
    await bot.delete_webhook(drop_pending_updates=True) 
    router.message.middleware(SimpleLoggerMiddleware())
    dp.include_router(router)
    dp.startup.register(on_startup)
    
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

if __name__ == '__main__':
    asyncio.run(main())
