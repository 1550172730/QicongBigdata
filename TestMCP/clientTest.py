import asyncio
from fastmcp import Client


"""
#简单的客户端测试代码

client = Client("http://localhost:8000/mcp")

async def call_tool(name: str):
    async with client:
        tools = await client.list_tools() #获取所有工具
        result = await client.call_tool("greet", {"name": name}) #调用工具并获取结果
        print(tools)
        print(result)

asyncio.run(call_tool("Ford"))
"""

#接入大语言模型