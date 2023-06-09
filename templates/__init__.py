
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

__all__ = ['arender', 'render']

import asyncio
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

this_dir = Path(__file__).parent
env = Environment(loader=FileSystemLoader(this_dir))


def render(file: str, context: dict[str, Any] = {}, **kwargs) -> str:
    template = env.get_template(file)
    return template.render(context, **kwargs)


async def arender(file: str, context: dict[str, Any] = {}, **kwargs) -> str:
    return await asyncio.to_thread(render, file, context, **kwargs)
