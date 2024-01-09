from aiogram import BaseMiddleware
import logging

class SimpleLoggerMiddleware(BaseMiddleware):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    async def __call__(self, handler, update, data):
        self.logger.info(f"Received update: {update}")
        return await handler(update, data)