from fastmcp import FastMCP

mcp = FastMCP("My MCP Server")
@mcp.tool(
    name="greet",           # Custom tool name for the LLM
    description="Generate greetings based on names", # Custom description
    tags={"greet",},      # Optional tags for organization/filtering
    meta={"version": "1.0", "author": "lqc"}  # Custom metadata
)
def greet(name: str) -> str:
    return f"Hello, {name}!"

if __name__ == "__main__":
    mcp.run(transport="http", port=8000)